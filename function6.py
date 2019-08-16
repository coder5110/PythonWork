import os
from os.path import basename
import pandas as pd
import pymysql.cursors
import pymysql
import boto3
from zipfile import ZipFile, ZIP_DEFLATED
import mimetypes
from email import encoders
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from jinja2 import Environment, FileSystemLoader



def Function6(job_id, logger, bonuscampaign_id, suffix, exportdefinitions_name):
    logger.info('start F6')
    
    # create work directory
    os.mkdir("/tmp/%s" % job_id)
    
    # initialize MySQL Client
    connection = pymysql.connect(host=os.environ['SQL_HOST'],
                                 user=os.environ['SQL_USER'],
                                 password=os.environ['SQL_PASSWORD'],
                                 db=os.environ['SQL_DBNAME'],
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    
    
    
    bonus_calculation_date = None
    sql = """
            SELECT MAX(date_calculated) as date_calculated
            FROM bonus_results
            WHERE bonuscampaign_id = %s
            LIMIT 1
            """ % bonuscampaign_id
    with connection.cursor() as cur:
        cur.execute(sql)
        result = cur.fetchone()
        bonus_calculation_date = result['date_calculated']
    
    
    
    # load SQL data to DataFrame
    sql = """
            SELECT 
                BR.zip AS PLZ,
                BR.city AS ORT,
                TA.type AS Sparte,
                TA.tariff_name AS RECHNUNGSBZEICHNUNG,
                TA.pid AS INTERNE_PRODUKTNUMMER,
                DATE_FORMAT(TA.available_bonus_from, '%%d.%%m.%%Y') AS PREIS_GUELTIG_AB,
                BR.consumption_from AS Stufe_ab,
                BR.consumption_until AS Stufe_bis,
                TA.kwhrate AS Arbeitspreis_HT,
                NULL AS ARBEITSPREIS_NT_NETTO,
                TA.basicrate AS Grundpreis,
                BR.nc AS Jahresbonus,
                NULL AS FREIMENGE_EINMALIG,
                "EBYD" AS VERTRIEBSGESELLSCHAFT,
                BR.ib AS SOFORTBONUS,
                TA.tac_id AS AGB
            FROM bonus_results BR 
            INNER JOIN (
            	SELECT bonuscampaign_id, MAX(date_calculated) date_calculated
            	FROM bonus_results
            	WHERE
            	    bonuscampaign_id = %s
            ) BR2
            ON 
            	BR.bonuscampaign_id = BR2.bonuscampaign_id 
            	AND BR.date_calculated = BR2.date_calculated
            INNER JOIN tariffs_available TA
                ON BR.pid = TA.pid
            WHERE BR.bonuscampaign_id = %s
            """ % (bonuscampaign_id, bonuscampaign_id)
    df = pd.read_sql(sql, connection)
    logger.debug('df.head() from SQL result:')
    logger.debug(df.head())
    
    
    
    # column name adjustment
    df.rename(columns={'Stufe_ab': 'Stufe ab', 'Stufe_bis': 'Stufe bis', 'Arbeitspreis_HT': 'Arbeitspreis HT'}, inplace=True)
    logger.debug('rename df columns name:')
    logger.debug(df.head())
    
    
    
    # split by type to Strom and Gas
    df_strom = df[df['Sparte'].str.contains('Strom')]
    df_gas   = df[df['Sparte'].str.contains('Gas')]
    logger.info('df_strom rows count: %d' % df_strom.shape[0])
    logger.info('df_gas rows count: %d' % df_gas.shape[0])
    
    
    
    # save DataFrame to CSV
    current_date = bonus_calculation_date
    current_year_str = current_date.strftime('%Y')
    current_date_str = current_date.strftime('%d%m%Y')
    
    csv_filename_strom = "EON_EPS_%s_öko_online_%s.csv" % (current_year_str, current_date_str)
    csv_filename_gas   = "EON_EPE_%s_öko_online_%s.csv" % (current_year_str, current_date_str)
    logger.debug('csv_filename_strom: %s' % csv_filename_strom)
    logger.debug('csv_filename_gas: %s' % csv_filename_gas)
    
    csv_fullpath_strom = "/tmp/%s/%s" % (job_id, csv_filename_strom)
    csv_fullpath_gas   = "/tmp/%s/%s" % (job_id, csv_filename_gas)
    logger.debug('csv_fullpath_strom: %s' % csv_fullpath_strom)
    logger.debug('csv_fullpath_gas: %s' % csv_fullpath_gas)
    
    df_strom.to_csv(path_or_buf=csv_fullpath_strom, encoding='cp1252', sep=';', decimal=',', index=False)
    df_gas.to_csv(path_or_buf=csv_fullpath_gas, encoding='cp1252', sep=';', decimal=',', index=False)
    
    
    
    # create ZIP of CSVs
    current_date_str = current_date.strftime('%Y%m%d')

    zip_filename = "Bonustabellen_%s.zip" % current_date_str
    s3_zip_filename = "KampagnenID_%s/%s" % (bonuscampaign_id, zip_filename)

    zip_fullpath = "/tmp/%s/%s" % (job_id, zip_filename)
    logger.info('zip_fullpath: %s' % zip_fullpath)
    
    file_paths = []
    if df_strom.shape[0] > 0:
        file_paths.append(csv_fullpath_strom)
    if df_gas.shape[0] > 0:
        file_paths.append(csv_fullpath_gas)
    
    with ZipFile(zip_fullpath, 'w', ZIP_DEFLATED) as zip:
        for file in file_paths:
            zip.write(file, basename(file))
        zip.close()
    
    
    
    # upload ZIP to S3
    s3 = boto3.client('s3')
    with open(zip_fullpath, 'rb') as fp:
        s3.put_object(
            Body=fp.read(),
            Bucket=os.environ['S3_BUCKET_BONUSFILES'],
            ContentType='application/zip',
            Key=s3_zip_filename,
            StorageClass='ONEZONE_IA'
        )
    
    
    
    # send email
    recipients = []
    sql = """
            SELECT * 
            FROM user 
            WHERE suffix = '%s'
            """ % suffix
    with connection.cursor() as cursor:
        cursor.execute(sql)
        recipients_raw = cursor.fetchall()
        logger.debug('email recipients:')
        logger.debug(recipients_raw)
        for r in recipients_raw:
            recipients.append(r['email_canonical'])
    
    
    COMMASPACE = ', '
    
    # Create the container (outer) email message.
    msg = MIMEMultipart()
    msg['Subject'] = "Bonusexport '%s' ist fertig gestellt" % exportdefinitions_name
    msg['From'] = "E.ON BonE by Friendly Data <noreply@eon-tools.de>"
    msg['To'] = COMMASPACE.join(recipients)
    msg.preamble = "Sehr geehrte Damen und Herren, der Bonusexport '%s' ist fertig gestellt" % exportdefinitions_name
    logger.info("E-mail Subjet: %s" % msg['Subject'])
    logger.info("E-mail From: %s" % msg['From'])
    logger.info("E-mail To: %s" % msg['To'])
    
    zip_file_size   = os.path.getsize(zip_fullpath) >> 20
    attach_zip_file = zip_file_size <= 9.5 
    logger.info("Attach ZIP file at e-mail?")
    logger.info(attach_zip_file)
    
    # the message body
    template_env = Environment(loader=FileSystemLoader('templates'))
    template     = template_env.get_template('function6.tmpl')
    body_text    = template.render(zip_attached=attach_zip_file)
    part = MIMEText(body_text, 'plain')
    msg.attach(part)
    
    logger.info("Current e-mail data:")
    logger.info(msg.as_string())

    # Open ZIP file in binary mode.
    if attach_zip_file:            
        with open(zip_fullpath, 'rb') as fp:
                ctype, encoding = mimetypes.guess_type(zip_filename)
                maintype, subtype = ctype.split('/', 1)
                afile = MIMEBase(maintype, subtype)
                afile.set_payload(fp.read())
                encoders.encode_base64(afile)
                afile.add_header('Content-Disposition', 'attachment', filename=zip_filename)
                msg.attach(afile)
    
    client = boto3.client('ses', region_name='eu-west-1')
    response = client.send_raw_email(
        RawMessage={
            'Data': msg.as_string(),
        }
    )