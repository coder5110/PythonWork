from datetime import datetime, timedelta

import os
import time

import boto3
import botocore
import numpy as np
import pandas as pd
import pymysql
from pytz import timezone
from sqlalchemy import create_engine


def round_up(value, step):
    rounded = np.ceil(value / step) * step
    return rounded


def round_down(value, step):
    rounded = np.floor(value / step) * step
    return rounded


def Function3(job_id, logger, bonuscalculation1_id, bonuscampaign_id, marketdata_ending, suffix):
    # these variables used to store mapping of integer value from optimized
    # DataFrames to original String value
    _col_pid = None
    _col_zip = None
    _col_city = None
    
    calculation_type = 'Strom' if marketdata_ending[0:5] == 'Strom' else 'Gas'
    
    logger.info("Start of Function3. Parameters:")
    logger.info('- - - - - - - - - - - - - -')
    logger.info("bonuscalculation1_id: %d" % bonuscalculation1_id)
    logger.info("bonuscampaign_id:     %d" % bonuscampaign_id)
    logger.info("marketdata_ending:    %s" % marketdata_ending)
    logger.info("suffix:               %s" % suffix)
    logger.info("calculation_type:     %s" % calculation_type)
    logger.info('- - - - - - - - - - - - - -')
    
    pd.set_option('display.max_columns', 500)
    
    
    
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(os.environ['S3_BUCKET_PARQUET'])
    
    germany = timezone('Europe/Berlin')
    current_date = datetime.now(germany)
    yesterday = datetime.now(germany) - timedelta(1)
    
    current_date_str = current_date.strftime('%Y%m%d')
    yesterday_str = yesterday.strftime('%Y%m%d')
    
    current_date_file = "%s_%s.parquet" % (current_date_str, marketdata_ending[:-4])
    yesterday_file = "%s_%s.parquet" % (yesterday_str, marketdata_ending[:-4])
    
    current_date_objs = list(bucket.objects.filter(Prefix=current_date_file))
    yesterday_objs = list(bucket.objects.filter(Prefix=yesterday_file))
    
    parquet_s3_key = None
    
    if len(current_date_objs) > 0:
        parquet_s3_key = current_date_file
    elif len(yesterday_objs) > 0:
        parquet_s3_key = yesterday_file
    else:
        raise ValueError('No parquet file!')
    
    logger.info("parquet_s3_key: %s" % parquet_s3_key)
    
    # create work directory
    os.mkdir("/tmp/%s" % job_id)
    
    file_name = '/tmp/%s/%s' % (job_id, parquet_s3_key)
    
    # download Parquet file
    try:
        s = time.time()
        s3_resource = boto3.resource('s3')
        s3_resource.Bucket(os.environ['S3_BUCKET_PARQUET']).download_file(parquet_s3_key, file_name)
        logger.info("Time to download Parquet from S3: %d" % (time.time() - s))
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logger.info("The object does not exist.")
            return
        else:
            raise
    
    # load Parquet file
    s = time.time()
    vxdata = pd.read_parquet(file_name,
                             engine='pyarrow',
                             columns=[
                                 'consumption',
                                 'zip',
                                 'city',
                                 'rank',
                                 'provider',
                                 'priceSumNet'
                             ])
    logger.info("Time to load Parquet to memory: %d" % (time.time() - s))
    
    conn = pymysql.connect(host=os.environ['SQL_HOST'],
                           user=os.environ['SQL_USER'],
                           password=os.environ['SQL_PASSWORD'],
                           db=os.environ['SQL_DBNAME'],
                           charset='utf8',
                           cursorclass=pymysql.cursors.DictCursor)
    
    
    
    # get SQL config
    sql = """
            SELECT
                DISTINCT BC2M.consumption_from,
                BC2M.consumption_until,
                BC1.handle_area_types_diff,
                BC2M.area_type,
                BC2M.abssteps,
                BC2M.max_bonus_sum_percentage,
                BC2M.max_bonus_sum_abs,
                BC2M.max_bonus_nc_abs,
                BC2M.max_bonus_nc_percentage,
                BC2M.min_bonus_ib_abs,
                BC2M.max_bonus_ib_abs,
                BC2M.lowest_rank,
                BC2M.highest_rank,
                BC2M.maxamortisation
            FROM bonus_calculation_2_market BC2M
            INNER JOIN bonus_calculation_1 BC1
                ON BC2M.bonuscalculation1_id = BC1.bonuscalculation1_id
            WHERE
                BC2M.bonuscalculation1_id = %d
            """ % bonuscalculation1_id
    config = pd.read_sql(sql, conn)
    
    # save SQL to CSV for tracing
    file_name = "/tmp/%s/%s" % (job_id, 'sql_1_config.csv')
    # config.to_csv(path_or_buf=file_name, index=False)
    
    
    
    # get SQL tariff
    sql = """
            SELECT
                TA.pid,
                TA.area_collection,
                TA.basicrate,
                TA.basicrate_margin,
                TA.kwhrate,
                TA.kwhrate_margin,
                TA.consumption_from,
                TA.consumption_until
            FROM tariffs_available TA
            INNER JOIN bonus_calculation_1 BC1
            ON BC1.product_ext_name = TA.tariff_name
            WHERE
                BC1.bonuscalculation1_id = %d
                AND TA.type = '%s'
                AND DATE(NOW()) BETWEEN
                    TA.available_bonus_from
                    AND TA.available_bonus_until
            """ % (bonuscalculation1_id, calculation_type)
    tariff = pd.read_sql(sql, conn)
    
    # save SQL to CSV for tracing
    file_name = "/tmp/%s/%s" % (job_id, 'sql_2_tariff.csv')
    # tariff.to_csv(path_or_buf=file_name, index=False)
    
    
    
    # tariff optimization
    _col_pid = pd.DataFrame({'pid': tariff.pid.unique(), 'pid_id': range(len(tariff.pid.unique()))})
    
    tariff = tariff.merge(_col_pid, on='pid', how='left')
    tariff.drop(columns=['pid'], axis=1, inplace=True)
    
    
    
    # get SQL costmodelinternal
    sql = """
            SELECT
                CMI.costmodelinternal_oneoff,
                CMI.costmodelinternal_pa
            FROM cost_model_internal CMI
            INNER JOIN bonus_calculation_1 BC1
                ON CMI.costmodelinternal_name = BC1.costmodelinternal_name
            WHERE
                BC1.bonuscalculation1_id = %d
                AND DATE(NOW()) BETWEEN
                    CMI.costmodelinternal_bonus_validfrom
                    AND CMI.costmodelinternal_bonus_validuntil
            """ % bonuscalculation1_id
    costmodelinternal = pd.read_sql(sql, conn)
    
    # save SQL to CSV for tracing
    file_name = "/tmp/%s/%s" % (job_id, 'sql_3_costmodelinternal.csv')
    # costmodelinternal.to_csv(path_or_buf=file_name, index=False)
    
    
    
    # get SQL costmodelprovision
    sql = """
            SELECT
                CMP.consumption_from,
                CMP.consumption_until,
                CMP.costmodelprovision_oneoff,
                CMP.costmodelprovision_pa
            FROM cost_model_provision CMP
            INNER JOIN bonus_calculation_1 BC1
                ON CMP.costmodelprovision_name = BC1.costmodelprovision_name
            WHERE
                BC1.bonuscalculation1_id = %d
                AND DATE(NOW()) BETWEEN
                    CMP.costmodelprovision_bonus_validfrom
                    AND CMP.costmodelprovision_bonus_validuntil
            """ % bonuscalculation1_id
    costmodelprovision = pd.read_sql(sql, conn)
    
    # save SQL to CSV for tracing
    file_name = "/tmp/%s/%s" % (job_id, 'sql_4_costmodelprovision.csv')
    # costmodelprovision.to_csv(path_or_buf=file_name, index=False)
    
    
    
    # get SQL areas
    sql = """
            SELECT zip, city, AA.area_collection, area_type
            FROM area_assignment AA
            INNER JOIN area_definition AD
                ON AA.area_collection = AD.area_collection
            WHERE
                AD.type = '%s'
                AND DATE(NOW()) BETWEEN
                    available_bonus_from
                    AND available_bonus_until
            """ % calculation_type
    areas = pd.read_sql(sql, conn)
    
    # save SQL to CSV for tracing
    file_name = "/tmp/%s/%s" % (job_id, 'sql_5_areas.csv')
    # areas.to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 1 - Eliminate duplicates from vxdata
    s = time.time()
    vxdata.drop_duplicates(subset=['consumption',
                                   'zip',
                                   'city',
                                   'rank'],
                           inplace=True)
    
    logger.info("Time for Step 1: %d" % (time.time() - s))
    # Step 2 - Save sample of DataFrame for tracing
    file_name = "/tmp/%s/%s" % (job_id, 'step_01.csv')
    # vxdata.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 2 - Delete rows where provider is E.ON Energie Deutschland GmbH
    s = time.time()
    vxdata.query("provider != 'E.ON Energie Deutschland GmbH'", inplace=True)
    vxdata.drop('provider', 1, inplace=True)
    
    logger.info("Time for Step 2: %d" % (time.time() - s))
    # Step 2 - Save sample of DataFrame for tracing
    file_name = "/tmp/%s/%s" % (job_id, 'step_02.csv')
    # vxdata.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 3 - Join vxdata (Parquet) with SQL areas & release memory from unused frames
    s = time.time()
    result = pd.merge(areas, vxdata, how='inner', on=['city', 'zip'])
    del areas
    del vxdata
    
    logger.info("Time for Step 3: %d" % (time.time() - s))
    # Step 3 - Save sample of DataFrame for tracing
    file_name = "/tmp/%s/%s" % (job_id, 'step_03.csv')
    # result.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 4 - Join result and config DataFrame
    s = time.time()
    result.sort_values(by=['consumption'], inplace=True)
    config.sort_values(by=['consumption_until'], inplace=True)
    config_column_stats = config.groupby('area_type').size()
    if len(config_column_stats) == 0:
        # Merge just based on consumption
        result = pd.merge_asof(result, config,
                               left_on='consumption',
                               right_on='consumption_until',
                               direction='forward')
        result.drop(columns=['consumption_from', 'consumption_until'],
                axis=1,
                inplace=True)
        logger.info("Time for Step 4, area difference False: %d" % (time.time() - s))
    else:
        # Merge on area_type and consumption
        result = pd.merge_asof(result, config,
                               left_on='consumption',
                               right_on='consumption_until',
                               by='area_type',
                               direction='forward')
        result.drop(columns=['consumption_from', 'consumption_until', 'area_type'],
                axis=1,
                inplace=True)
        logger.info("Time for Step 4, area difference True: %d" % (time.time() - s))
    # Step 4 - Save sample of DataFrame for tracing
    file_name = "/tmp/%s/%s" % (job_id, 'step_04.csv')
    # result.head(n=1000000).to_csv(path_or_buf=file_name, index=False)
    
    del config
    

    
    # Step 5 - deprecated
    
    
    
    # Step 6 - Optimize string columns at result DataFrame by
    #            replacing String object with Integer.
    # note: keep mapping of original String value to Integer for later use
    #       when inserting result to MySQL
    s = time.time()
    _col_zip = pd.DataFrame({'zip': result.zip.unique(), 'zip_id': range(len(result.zip.unique()))})
    _col_city = pd.DataFrame({'city': result.city.unique(), 'city_id': range(len(result.city.unique()))})
    result = result.merge(_col_zip, on='zip', how='left')
    result = result.merge(_col_city, on='city', how='left')
    result.drop(columns=['zip', 'city'], axis=1, inplace=True)
    logger.info("Time for Step 6: %d" % (time.time() - s))
    
    
    
    # Step 7 - Join tariff DataFrame to result DataFrame.
    s = time.time()
    result = pd.merge(result, tariff, how='inner', on='area_collection')
    result.query('consumption_from <= consumption <= consumption_until', inplace=True)
    result.drop(columns=['consumption_from'], axis=1, inplace=True)
    result.rename(columns={'consumption_until': 'consumption_until_from_tariff'}, inplace=True)
    
    # Step 7 - Release memory from unused variable(s)
    del tariff
    logger.info("Time for Step 7: %d" % (time.time() - s))
    
    # Step 7 - Save sample of DataFrame for tracing
    file_name = "/tmp/%s/%s" % (job_id, 'step_07.csv')
    # result.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 8 - Add two new columns
    s = time.time()
    result['costmodelinternal_oneoff'] = costmodelinternal.iloc[0]['costmodelinternal_oneoff']
    result['costmodelinternal_pa'] = costmodelinternal.iloc[0]['costmodelinternal_pa']
    
    # Step 8 - Release memory from unused variable(s)
    del costmodelinternal
    
    logger.info("Time for Step 8: %d" % (time.time() - s))
    # Step 8 - Save sample to CSV for tracing
    file_name = "/tmp/%s/%s" % (job_id, 'step_08.csv')
    # result.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 9 - Merging costmodelprovision to result
    s = time.time()
    costmodelprovision.sort_values(by=['consumption_until'], inplace=True)
    result.sort_values(by=['consumption'], inplace=True)
    result = pd.merge_asof(result, costmodelprovision, left_on='consumption', right_on='consumption_until', direction='forward')
    
    del costmodelprovision
    
    logger.info("Time for Step 9: %d" % (time.time() - s))
    # Step 9 - Save sample to CSV for tracing
    file_name = "/tmp/%s/%s" % (job_id, 'step_09.csv')
    # result.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 10 - Calculate eonprice column
    s = time.time()
    result.eval('eonprice = basicrate + consumption * kwhrate', inplace=True)
    result['eonprice'].round(2)
    
    logger.info("Time for Step 10: %d" % (time.time() - s))
    file_name = "/tmp/%s/%s" % (job_id, 'step_10.csv')
    # result.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 11
    # - pick smaller value of ( max_bonus_sum_abs , eonprice*max_bonus_sum_percentage )
    # - then round_down with value from abssteps column
    # - drop unused columns
    s = time.time()
    result.eval('tmp1 = eonprice * max_bonus_sum_percentage', inplace=True)
    result['max_bonus_sum_abs'] = round_down(result[['tmp1', 'max_bonus_sum_abs']].min(axis=1), result['abssteps'])
    result.drop(columns=['max_bonus_sum_percentage', 'tmp1'],
                axis=1,
                inplace=True)
    
    logger.info("Time for Step 11: %d" % (time.time() - s))
    file_name = "/tmp/%s/%s" % (job_id, 'step_11.csv')
    # result.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 12
    # - pick smaller value of ( max_bonus_nc_abs , eonprice*max_bonus_nc_percentage )
    # - then round_down with value from abssteps column
    s = time.time()
    result.eval('tmp1 = eonprice * max_bonus_nc_percentage', inplace=True)
    result['tmp1'] = round_down(result['tmp1'], result['abssteps'])
    result['max_bonus_nc_abs'] = result[['tmp1', 'max_bonus_nc_abs']].min(axis=1)
    result.drop(columns=['max_bonus_nc_percentage', 'tmp1'],
                axis=1,
                inplace=True)
    
    logger.info("Time for Step 12: %d" % (time.time() - s))
    file_name = "/tmp/%s/%s" % (job_id, 'step_12.csv')
    # result.head(n=100000).to_csv(path_or_buf=file_name, index=False)
     
    
    
    # Step 13
    # - redefine max_bonus_sum_abs
    # - delete all rows where ( eonprice – max_bonus_sum_abs ) > priceSumNet
    s = time.time()
    result.eval('tmp1 = max_bonus_nc_abs + max_bonus_ib_abs', inplace=True)
    result['max_bonus_sum_abs'] = result[['tmp1', 'max_bonus_sum_abs']].min(axis=1)
    result.query('(eonprice - max_bonus_sum_abs) <= priceSumNet', inplace=True)
    result.drop(columns=['tmp1'],
                axis=1,
                inplace=True)
    
    logger.info("Time for Step 13: %d" % (time.time() - s))
    file_name = "/tmp/%s/%s" % (job_id, 'step_13.csv')
    # result.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 14 - Delete all rows where rank < highest_rank
    s = time.time()
    result.query('rank >= highest_rank', inplace=True)
    
    logger.info("Time for Step 14: %d" % (time.time() - s))
    file_name = "/tmp/%s/%s" % (job_id, 'step_14.csv')
    # result.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 15
    # - calculate max_bonus_pm_abs column
    # - then round_down with value from abssteps column
    s = time.time()
    result.eval('max_bonus_pm_abs = (maxamortisation * (basicrate_margin + consumption * kwhrate_margin - costmodelinternal_pa - costmodelprovision_pa)) - costmodelinternal_oneoff - costmodelprovision_oneoff', inplace=True)
    result.eval('zero = 0', inplace=True)
    result['max_bonus_pm_abs'] = result[['max_bonus_pm_abs', 'zero']].max(axis=1)
    result['max_bonus_pm_abs'] = round_down(result['max_bonus_pm_abs'], result['abssteps'])
    result.drop(columns=['zero'],
                axis=1,
                inplace=True)
    
    logger.info("Time for Step 15: %d" % (time.time() - s))
    file_name = "/tmp/%s/%s" % (job_id, 'step_15.csv')
    # result.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 16 - Reduce result to less columns at the moment
    s = time.time()
    select_columns = [
        'pid_id',
        'zip_id',
        'city_id',
        'rank',
        'consumption',
        'consumption_until_from_tariff',
        'eonprice',
        'priceSumNet',
        'max_bonus_sum_abs',
        'max_bonus_ib_abs',
        'min_bonus_ib_abs',
        'lowest_rank',
        'max_bonus_pm_abs',
        'abssteps',
        'max_bonus_nc_abs'
    ]
    result = result[select_columns]
    
    # Step 16 - Rename columns
    result.rename(columns={'consumption': 'consumption_from', 'consumption_until_from_tariff': 'consumption_until'}, inplace=True)
    logger.info("Time for Step 16: %d" % (time.time() - s))
    file_name = "/tmp/%s/%s" % (job_id, 'step_16.csv')
    # result.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 17 - Calculate bonus_needed column
    s = time.time()
    result.eval('bonus_needed = eonprice - priceSumNet', inplace=True)
    result.eval('zero = 0', inplace=True)
    result['bonus_needed'] = result[['bonus_needed', 'zero']].max(axis=1)
    result['bonus_needed'] = round_up(result['bonus_needed'], result['abssteps'])
    result.drop(columns=['zero'],
                axis=1,
                inplace=True)
    logger.info("Time for Step 17: %d" % (time.time() - s))
    file_name = "/tmp/%s/%s" % (job_id, 'step_17.csv')
    # result.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 18 - Create result columns
    s = time.time()
    result.eval('zero = 0', inplace=True)
    
    # Step 19 - Find instant bonus (ib) based on minimum
    result.eval('ib = min_bonus_ib_abs', inplace=True)
    result['ib'] = round_up(result['ib'], result['abssteps'])
    
    # Step 20 - Find new customer bonus (nc) based on what's still needed after minimum ib
    result.eval('nc = bonus_needed - ib', inplace=True)
    result['nc'] = round_up(result['nc'], result['abssteps'])
    
    # Step 21 - Make sure, nc is not below 0 and not over maximum nc
    result['nc'] = result[['nc', 'zero']].max(axis=1)
    result['nc'] = result[['nc', 'max_bonus_nc_abs']].min(axis=1)
    
    # Step 22 - If there is bonus needed left, get it through instant bonus (ib)
    result.eval('ib = bonus_needed - nc', inplace=True)
    result['ib'] = round_up(result['ib'], result['abssteps'])
    
    result.drop(columns=['zero'],
                axis=1,
                inplace=True)
    
    logger.info("Time for Steps 18-22: %d" % (time.time() - s))
    file_name = "/tmp/%s/%s" % (job_id, 'step_22.csv')
    # result.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 23 - Drop all rows in result where: ( nc + ib ) < bonus_needed
    s = time.time()
    result.query('(nc + ib) >= bonus_needed', inplace=True)
    
    logger.info("Time for Step 23: %d" % (time.time() - s))
    file_name = "/tmp/%s/%s" % (job_id, 'step_23.csv')
    # result.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    

    # Step 24 - Keep all rows for pid/zip/city/consumption where rank is the lowest
    s = time.time()
    # COMMENTED IS THE BREAKING IMPLEMENTATION BY SUSHMIT
    # groupby_columns = [
    #                     pd.Grouper('pid_id', axis=1),
    #                     pd.Grouper('zip_id', axis=1),
    #                     pd.Grouper('city_id', axis=1),
    #                     pd.Grouper('consumption_from', axis=1)
    # ]
    # step24 = result.groupby(groupby_columns, as_index=False)
    # NEXT LINE IS THE OLD WORKING IMPLEMENTATION
    step24 = result.groupby(['pid_id', 'zip_id', 'city_id', 'consumption_from'], as_index=False)
    step24 = step24['rank'].min()
    step24 = result.merge(step24, how='inner', on=['pid_id', 'zip_id', 'city_id', 'consumption_from', 'rank'])
    
    logger.info("Time for Step 24: %d" % (time.time() - s))
    file_name = "/tmp/%s/%s" % (job_id, 'step_24.csv')
    # result.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 25 - Keep all rows for pid/zip/city/consumption where bonus_needed <= max_bonus_pm_abs & rank <= lowest_rank
    # 1. We filter for rank <= lowest_rank.
    # 2. Calculate max_bonus_pm_abs - bonus_needed
    # 3. Only keep where 2. is >= 0
    s = time.time()
    result.query('rank <= lowest_rank & 0 >= max_bonus_pm_abs - bonus_needed', inplace=True)
    
    logger.info("Time for Step 25: %d" % (time.time() - s))
    file_name = "/tmp/%s/%s" % (job_id, 'step_25.csv')
    # result.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 26
    # Combine dataframes from steps 24 & 25 to new result DataFrame and keep where bonus_needed is minimal
    s = time.time()
    result = pd.concat([step24, result])
    del step24
    
    result.drop_duplicates(inplace=True)
    
    # ROLLBACK ALSO HERE
    tmp = result.groupby(['pid_id', 'zip_id', 'city_id', 'consumption_from'], as_index=False)
    tmp = tmp['bonus_needed'].min()
    result = result.merge(tmp, how='inner', on=['pid_id', 'zip_id', 'city_id', 'consumption_from', 'bonus_needed'])
    del tmp
    
    # ROLLBACK ALSO HERE
    tmp = result.groupby(['pid_id', 'zip_id', 'city_id', 'consumption_from'], as_index=False)
    tmp = tmp['rank'].min()
    result = result.merge(tmp, how='inner', on=['pid_id', 'zip_id', 'city_id', 'consumption_from', 'rank'])
    del tmp
    
    logger.info("Time for Step 26: %d" % (time.time() - s))
    file_name = "/tmp/%s/%s" % (job_id, 'step_26.csv')
    # result.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 27
    # Recalculate these columns in this order one after another:
    # a. nc = smaller one of: (bonus_needed – ib), ( max_bonus_nc_abs )
    # b. ib = bonus_needed – nc
    s = time.time()
    result.eval('tmp = bonus_needed - ib', inplace=True)
    result['nc'] = result[['max_bonus_nc_abs', 'tmp']].min(axis=1)
    result.eval('ib = bonus_needed - nc')
    
    logger.info("Time for Step 27: %d" % (time.time() - s))
    file_name = "/tmp/%s/%s" % (job_id, 'step_27.csv')
    # result.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 28
    # Set consumption_until like the next higher consumption_from
    # in the pid-zip-city group minus 1.
    # If it is the highest, keep the existing value.
    s = time.time()
    
    # for each group of pid-zip-city, sort rows by consumption_from (ascending)
    # COMMENTED IS THE BREAKING IMPLEMENTATION BY SUSHMIT
    # pid_zip_city_columns = [
    #                         pd.Grouper('pid_id', axis=1),
    #                         pd.Grouper('zip_id', axis=1),
    #                         pd.Grouper('city_id', axis=1)
    # ]
    # result = result.groupby(pid_zip_city_columns).apply(pd.DataFrame.sort_values, 'consumption_from')
    # FOLLOWING LINE IS THE ROLLBACK TO THE WORKING VERSION
    result = result.groupby(['pid_id', 'zip_id', 'city_id']).apply(pd.DataFrame.sort_values, 'consumption_from')
    
    # WRONG ONE AGAIN
    # result['consumption_until_new'] = result.groupby(pid_zip_city_columns)['consumption_from'].shift(-1)
    
    # WORKING ONE
    result['consumption_until_new'] = result.groupby(['pid_id', 'zip_id', 'city_id'])['consumption_from'].shift(-1)
    result['consumption_until_new'] -= 1
    result['consumption_until_new'] = result['consumption_until_new'].fillna(result['consumption_until'])
    result.drop(['consumption_until'], inplace=True, axis=1)
    result.rename(index=str, inplace=True, columns={'consumption_until_new': 'consumption_until'})
    
    logger.info("Time for Step 28: %d" % (time.time() - s))
    file_name = "/tmp/%s/%s" % (job_id, 'step_28.csv')
    # result.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 29 - Reduce the result dataframe columns
    s = time.time()
    result = result[['pid_id', 'zip_id', 'city_id', 'consumption_from', 'consumption_until', 'nc', 'ib']]
    
    logger.info("Time for Step 29: %d" % (time.time() - s))
    file_name = "/tmp/%s/%s" % (job_id, 'step_29.csv')
    # result.head(n=100000).to_csv(path_or_buf=file_name, index=False)
    
    
    
    # Step 30 - Insert calculation result to MySQL, bonus_results table
    # Pre: 
    #    Delete existing rows at bonus_results (if any), that:
    #    (date_calculated = today) for current bonuscalculation1_id
    s = time.time()
    
    # Pre - delete existing rows
    sql = """
            DELETE FROM bonus_results
            WHERE 
                date_calculated = DATE(NOW())
                AND bonuscalculation1_id = %s
            """ % bonuscalculation1_id
    with conn.cursor() as cursor:
        cursor.execute(sql)
        conn.commit()
    
    connection_string = "mysql+mysqlconnector://%s:%s@%s:3306/%s" % (os.environ['SQL_USER'], os.environ['SQL_PASSWORD'], os.environ['SQL_HOST'], os.environ['SQL_DBNAME'])
    engine = create_engine(connection_string, echo=False, pool_recycle=30)
    result = result.merge(_col_zip, how='inner', on='zip_id')
    result = result.merge(_col_city, how='inner', on='city_id')
    result = result.merge(_col_pid, how='inner', on='pid_id')
    result.drop(columns=['zip_id', 'city_id', 'pid_id'], axis=1, inplace=True)
    result['bonuscampaign_id'] = bonuscampaign_id
    result['bonuscalculation1_id'] = bonuscalculation1_id
    result['date_calculated'] = pd.to_datetime('today')
    result.to_sql(name='bonus_results', con=engine, if_exists='append', index=False, chunksize=30000)
    
    del _col_zip
    del _col_city
    del _col_pid
    
    logger.info("Time for Step 30: %d" % (time.time() - s))
    
    
    
    sql = """
            INSERT INTO `billing` (
                `inserted`,
                `department_responsible`,
                `billing_name`,
                `amount`
            ) VALUES (NOW(), '%s', 'bonuscalculation %s', %d)
            """ % (suffix, bonuscalculation1_id, result.shape[0])
    with conn.cursor() as cursor:
        cursor.execute(sql)
        conn.commit()
    
    del result
    
    logger.info("Calculation info has been inserted to `billing` MySQL table")
    logger.info("F3, done.")
    
    # end of Function3
