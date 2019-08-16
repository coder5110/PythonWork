import os
import shutil
import glob
import json
import boto3
import uuid
from zipfile import ZipFile
from datetime import datetime
import pytz
from pytz import timezone
import pymysql.cursors
import paramiko
import pandas as pd
import time



def Function1(job_id, logger, remote_path, marketdata_ending):
    """
    Function1 task:
    - download ZIP files from SFTP server
    - upload it to ZIP-bucket at Amazon S3
    - create Parquet file from ZIP of CSV files
    - upload it to Parquet-bucket at Amazon S3
    """
    
    # to get current date file name prefix
    germany = timezone('Europe/Berlin')
    current_date = datetime.now(germany)
    current_date_str = current_date.strftime('%Y%m%d')
    
    # s3 client to upload downloaded files from SFTP
    s3 = boto3.client('s3')
    
    # create work directory
    os.mkdir("/tmp/%s" % job_id)
    
    # initialize SSH Client
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=os.environ['SFTP_HOST'],
                       username=os.environ['SFTP_USER'],
                       password=os.environ['SFTP_PASSWORD'])
    
    # initialize SFTP Client as child of SSH Client
    sftp_client = ssh_client.open_sftp()
    
    
    
    # download ZIP file from SFTP
    s = time.time()
    local_path  = "/tmp/%s/%s" % (job_id, marketdata_ending)
    sftp_client.get(remote_path, local_path)
    logger.info("Time to download via SFTP: %d" % (time.time() - s))
    
    # close SFTP connection
    sftp_client.close()
    
    # upload ZIP to Amazon S3
    s = time.time()
    s3_key = "%s_%s" % (current_date_str, marketdata_ending)
    s3.upload_file(local_path, os.environ['S3_BUCKET_ORIGIN'], s3_key)
    logger.info("Time to upload to S3: %d" % (time.time() - s))
    
    
    
    # extract the ZIP file
    s = time.time()
    zip_path     = "/tmp/%s/%s"         % (job_id, marketdata_ending)
    extract_path = "/tmp/%s/extracted/" % (job_id)
    zip_ref = ZipFile(zip_path, 'r')
    zip_ref.extractall(extract_path)
    zip_ref.close()
    logger.info("Time to extract ZIP file: %d" % (time.time() - s))
    
    
    
    # open all CSV files, combine it and save as Parquet.
    parquet_file_name = "/tmp/%s/%s_%s.parquet" % (job_id, current_date_str, marketdata_ending[:-4])
    convert_csv_to_parquet(job_id, logger, marketdata_ending, current_date_str, parquet_file_name)
    
    # upload Parquet file to S3 bucket
    s = time.time()
    s3_key = os.path.basename(parquet_file_name)
    s3.upload_file(parquet_file_name, os.environ['S3_BUCKET_PARQUET'], s3_key)
    logger.info("Time to upload Parquet file: %d" % (time.time() - s))
    
    
    
    # delete work directory
    shutil.rmtree("/tmp/%s" % job_id)



def convert_csv_to_parquet(job_id, logger, marketdata_ending, current_date_str, parquet_file_name):
    """
    This function will:
    - read list of CSV files
    - utilize read_a_csv_file() function to get Pandas DataFrame of each CSV file
    - concat all DataFrame to a single DataFrame
    - save the single DataFrame to Parquet
    """
    
    csv_files = glob.glob("/tmp/%s/extracted/*.csv" % job_id)
    
    # open each CSV file and store it at single list
    data_frames = []
    for csv_file in csv_files:
        data_frame = read_a_csv_file(job_id, logger, csv_file)
        data_frames.append(data_frame)
    
    # concat all DataFrame as new single DataFrame
    s = time.time()
    combined_data_frame = pd.concat(data_frames, ignore_index=False)
    logger.info("Time to pd.concat: %d" % (time.time() - s))

    # drop duplicates
    s = time.time()
    combined_data_frame.drop_duplicates(subset=['consumption',
                                                'zip',
                                                'city',
                                                'rank'],
                                        inplace=True)
    logger.info("Time to remove duplicates: %d" % (time.time() - s))
    
    # change date format
    s = time.time()
    date_column_stats = combined_data_frame.groupby('date').size()
    logger.info("Date uniqueness calculation time: %d" % (time.time() - s))
    
    s = time.time()
    if len(date_column_stats) == 1:
        # optimized date re-format: only assign one value
        original_date = None
        for k,v in date_column_stats.items():
            original_date = k
        combined_data_frame['date'] = datetime.strptime(original_date, '%d.%m.%Y').strftime('%Y%m%d')
        logger.info("Time to reformate date column with optimized way: %d" % (time.time() - s))
    else:
        # fallback: re-format date column values one-by-one
        combined_data_frame['date'] = pd.to_datetime(combined_data_frame['date'], format='%d.%m.%Y', errors='coerce').dt.strftime('%Y%m%d')
        logger.info("Time to reformate date column: %d" % (time.time() - s))
    
    type_column = 'Strom' if 'Strom' in marketdata_ending else 'Gas'
    targetGroup_column = None
    if 'Gewerbe' in marketdata_ending:
        targetGroup_column = 'Gewerbe'
    elif 'Privat' in marketdata_ending:
        targetGroup_column = 'Privat'
    elif 'Heiz' in marketdata_ending:
        targetGroup_column = 'Heiz'

    pricesNet_column = None
    if 'Gewerbe' in marketdata_ending:
        pricesNet_column = True
    elif 'Privat' in marketdata_ending:
        pricesNet_column = False
    elif 'Heiz' in marketdata_ending:
        pricesNet_column = False
    
    combined_data_frame['type'] = type_column
    combined_data_frame['targetGroup'] = targetGroup_column
    combined_data_frame['pricesNet'] = pricesNet_column
    
    combined_data_frame = combined_data_frame[[
        'date',
        'type',
        'targetGroup',
        'pricesNet',
        'consumption',
        'zip',
        'city',
        'households',
        'rank',
        'provider',
        'tariffName',
        'priceSumNet',
        'basicRate',
        'kwhSum',
        'NCbonus',
        'Ibonus',
        'kwhRate'
        ]]
    
    # divide kwhRate by 100
    combined_data_frame['kwhRate'] = combined_data_frame['kwhRate'] / 100
    
    # save it as Parquet file
    s = time.time()
    combined_data_frame.to_parquet(parquet_file_name, engine='pyarrow', compression='gzip')
    logger.info("Time to save Parquet file: %d" % (time.time() - s))



def read_a_csv_file(job_id, logger, csv_file_path):
    """
    This function will:
    - read single CSV file
    - read selected columns
    - rename columns title to English
    - return Pandas DataFrame of formatted CSV file
    """
    
    df_tmp = pd.read_csv(csv_file_path,
                         encoding='cp1252',
                         engine='c',
                         sep=';',
                         decimal=',',
                         header=0,
                         nrows=1)
    
    netto_brutto = 'netto'
    if 'Gesamtkosten (brutto) in EUR pro Jahr' in df_tmp.columns:
        netto_brutto = 'brutto'
    
    gesamtkosten    = "Gesamtkosten (%s) in EUR pro Jahr" % netto_brutto
    grundpreis      = "Grundpreis (%s) in EUR pro Jahr" % netto_brutto
    verbrauchspreis = "Verbrauchspreis (%s) in EUR pro Jahr" % netto_brutto
    neukundenbonus  = "Neukundenbonus in EUR (%s)" % netto_brutto
    sofortbonus     = "Sofortbonus in EUR (%s)" % netto_brutto
    
    kwhRate_column_name = "Arbeitspreis in ct/kWh (%s)" % netto_brutto
    if 'Strom_' in csv_file_path:
        kwhRate_column_name = "Arbeitspreis HT in ct/kWh (%s)" % netto_brutto
    
    # only read selected columns
    s = time.time()
    df = pd.read_csv(csv_file_path,
                     encoding='cp1252',
                     engine='c',
                     sep=';', 
                     decimal=',',
                     header=0,
                     dtype={
                         'Verbrauchsstufe in kWh': int, 
                         'Postleitzahl': str, 
                         'Ort': str, 
                         'Anzahl Haushalte': int, 
                         'Platz': int, 
                         'Anbietername': str, 
                         'Tarifname': str, 
                         gesamtkosten: float, 
                         grundpreis: float, 
                         verbrauchspreis: float, 
                         neukundenbonus: float, 
                         sofortbonus: float, 
                         kwhRate_column_name: float,
                         'Exportdatum': str,
                     },
                     usecols=[
                         'Verbrauchsstufe in kWh', 
                         'Postleitzahl', 
                         'Ort', 
                         'Anzahl Haushalte', 
                         'Platz', 
                         'Anbietername', 
                         'Tarifname', 
                         gesamtkosten, 
                         grundpreis, 
                         verbrauchspreis, 
                         neukundenbonus, 
                         sofortbonus, 
                         kwhRate_column_name,
                         'Exportdatum',
                     ])
    logger.info("Time to load CSV file: %d" % (time.time() - s))
    
    # change column title to English
    df.columns = [
        'consumption',
        'zip',
        'city',
        'households',
        'rank',
        'provider',
        'tariffName',
        'priceSumNet',
        'basicRate',
        'kwhSum',
        'NCbonus',
        'Ibonus',
        'kwhRate',
        'date'
    ]
    
    return df