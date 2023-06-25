import asyncio
import csv
import gc
import io
import json
import logging
import os
import sys
import time
import warnings
import pmfby
import zipfile
from copy import deepcopy
from datetime import date, datetime
from io import StringIO
from threading import Thread

import boto3
import connectorx as cx
import flask
import numpy as np
import pandas as pd
import pymysql
import requests
import xlsxwriter
from flask import (Blueprint, Response, jsonify, render_template, request,
                   send_file, send_from_directory)
from joblib import Parallel, delayed
from pymysqlpool.pool import Pool
from sqlalchemy import create_engine
from connection_master import (BASE_URL, connection_string, database_name,
                               endpoint, file_path, live_connection_string,
                               live_database_name, live_endpoint,
                               live_password, live_username, password,
                               read_connection_string, s3, s3_feedback_path,
                               s3_user_file_path, s3_report_path, session, username)
from custom_thread import SARUS_thread
from reporting_thread import Reporting_thread
from custom_thread_accounts import SARUS_accounting_thread
from dfvaliditycheck import CheckValidity

empRow = None
# import aiohttp

collection = Blueprint('collection', __name__)
LOG = logging.getLogger(__name__)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)


pool = Pool(host=endpoint, port=3306, user=username,
            password=password, db=database_name, local_infile=1)
pool.init()

pv_coll_row = ['SR NO', 'DEBIT TYPE*', 'PAY TYPE*', 'GROSS AMOUNT*', 'REFUND*', 'CHARGEBACK', 'LATE RETURN', 'CHARGES', 'SERVICETAX', 'SURCHARGE', 'TDS',	'NET AMOUNT*', 'PV NO*', 'FILE DATE*', 'MERCHANT NAME*', 'PAYMENT REPORT VOUCHER*',
               'ICNAME', 'ID', 'SEASON', 'YEAR', 'REMARKS']

tid_coll_row = ["ID", "BILLERNAME", "DEBITTYPE", "NARRATION", "PAYMODE", "PRODUCTCODE", "BDREFNO", "OURID", "REF1", "REF2", "REF3", "REF4",
                "CREATEDON", "GROSSAMOUNT",	"CHARGES", "SERVICETAX", "SURCHARGE", "TDS", "NETAMOUNT", "FILENAME", "SCHEME", "SEASON", "YEAR",
                "STATE", "SOURCE", "POLICYID", "BANKNAME", "BRANCHCODE", "BRANCHID", "BRANCHNAME", "IFSC", "REFUND", "DUPLICATE"]

def pv_collection_upload_thread(user_id, upload_log_id):

    try:
        print("1", upload_log_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        time_now = datetime.now()
        global pv_coll_row, pool
        print(user_id,  upload_log_id)

        # *****Creating connection*****
        try:
            conn = pool.get_conn()
            with conn.cursor() as cur:
                query = "select now()"
                cur.execute(query)
                n = cur.fetchone()
        except:
            print(upload_log_id, 'Trying to reconnect')
            pool.init()
            conn = pool.get_conn()

        # *****Database entry for process starts*****

        with conn.cursor() as cur:
            query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE=0, PROCESS_START_TIME=now() where ID=" + \
                str(upload_log_id)
            print(upload_log_id, query)
            cur.execute(query)
            conn.commit()

        with conn.cursor() as cur:
            query = "select FILETYPE from PMFBY.FILEUPLOAD_LOGS where ID=" + \
                str(upload_log_id)
            print(upload_log_id, query)
            cur.execute(query)
            result = cur.fetchone()
        filetype = result['FILETYPE']
        if filetype not in ['excel', 'csv']:
            with conn.cursor() as cur:
                query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Invalid file type' where id = " + \
                    str(upload_log_id)+";"
                print("Query: ", query)
                cur.execute(query)
                conn.commit()
            pool.release(conn)
            gc.collect()
            return
        time_now = datetime.now()
        time_get = time_now.strftime("%Y%m%d%H%M%S")
        updated_date_time = time_now.strftime("%Y-%m-%d %H:%M:%S")
        file_name = 'pv_coll'+str(user_id)+"_" + "_"+str(time_get)+".csv"

        pv_collection_data = pmfby.read_files_from_s3(upload_log_id, filetype)
        if type(pv_collection_data) is dict:
            with conn.cursor() as cur:
                e_str = str(pv_collection_data["error"]).replace('"', '\\"')
                e_str = e_str.replace("'", "\\'")
                query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Error while reading file', EXCEPTION = '" + e_str + "' where id = " + \
                    str(pv_collection_data)+";"
                print("Query: ", query)
                cur.execute(query)
                conn.commit()
            pool.release(conn)
            gc.collect()
            return

        elif pv_collection_data.shape[0] == 0:
            with conn.cursor() as cur:
                query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Upload file(s) is/are empty' where id = " + \
                    str(upload_log_id)+";"
                print("Query: ", query)
                cur.execute(query)
                conn.commit()
            pool.release(conn)
            gc.collect()
            return

        print(upload_log_id, pv_collection_data.shape[0])

        ######### Check if headers don't match #########
        columns = pv_coll_row
        headers = list(pv_collection_data.columns.values)
        headers.remove('FILENAME')
        # print("headerssssss s3file",headers)
        # print("columnsssssss",columns)

        try:
            if(headers != columns):
                with conn.cursor() as cur:
                    query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Headers do not match' where id = " + \
                        str(upload_log_id)+";"
                    print("Query: ", query)
                    cur.execute(query)
                    conn.commit()
                pool.release(conn)
                gc.collect()
                return
        except Exception as e:
            print("Exception Occurred")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print("Exception: ", e)
            e_str = str(e).replace('"', '\\"')
            e_str = e_str.replace("'", "\\'")
            with conn.cursor() as cur:
                query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Something went wrong', EXCEPTION = '" + \
                    str(e_str)+"' where id = "+str(upload_log_id)+";"
                print("Query: ", query)
                cur.execute(query)
                conn.commit()
            pool.release(conn)
            gc.collect()
            return

        dictionary = {'SR NO': 'SR_NO', 'DEBIT TYPE*': 'DEBIT_TYPE', 'PAY TYPE*': 'PAY_TYPE', 'GROSS AMOUNT*': 'GROSS_AMOUNT',
                      'REFUND*': 'REFUND', 'CHARGEBACK': 'CHARGEBACK', 'LATE RETURN': 'LATE_RETURN', 'CHARGES': 'CHARGES',
                      'SERVICETAX': 'SERVICETAX', 'SURCHARGE': 'SURCHARGE', 'TDS': 'TDS', 'NET AMOUNT*': 'NET_AMOUNT',
                      'PV NO*': 'PV_NO', 'FILE DATE*': 'FILE_DATE', 'MERCHANT NAME*': 'MERCHANT_NAME',
                      'PAYMENT REPORT VOUCHER*': 'PAYMENT_REPORT_VOUCHER', 'ICNAME': 'ICNAME', 'ID': 'ID',
                      'SEASON': 'SEASON', 'YEAR': 'YEAR', 'REMARKS': 'REMARKS'}

        pv_collection_data.rename(columns=dictionary, inplace=True)

        no_of_records_uploaded = pv_collection_data.shape[0]

        pv_collection_data['UPLOAD_TASK_ID'] = upload_log_id

        pv_collection_data['UPDATED_USERID'] = user_id

        pv_collection_data['UPDATED_DATE'] = updated_date_time

        pv_collection_data = pv_collection_data.apply(
            lambda x: x.astype(str).str.upper().str.strip())

        pv_collection_data = pv_collection_data.replace(
            '\\\\', '\\\\\\\\', regex=True)

        # pv_collection_data[['CROP_CODE', 'STATE_CODE', 'L3_NAME_CODE', 'L4_NAME_CODE', 'L5_NAME_CODE', 'L6_NAME_CODE', 'L7_NAME_CODE']] = pv_collection_data[['CROP_CODE', 'STATE_CODE', 'L3_NAME_CODE', 'L4_NAME_CODE', 'L5_NAME_CODE', 'L6_NAME_CODE', 'L7_NAME_CODE']].apply(
        #     lambda x: x.str.lstrip('0'))

        pv_collection_data = pv_collection_data.replace('', None)

        duplicate_list = ['PV_NO']
        # de_duplicate_list = ['PREMIUM_RATE_PCT', 'FARMER_SHARE_PCT', 'GOI_SHARE_PCT', 'STATE_SHARE_PCT',
        #                      'INDEMNITY', 'SUM_INSURED', 'CUT_OFF_DATE', 'NOTIFIED_IU_NAME', 'INSURANCE_UNIT_NAME']

        valuesToBeChecked = {
            'DEBIT_TYPE': None,
            'PAY_TYPE': None,
            'PV_NO': None,
            'FILE_DATE': None,
            'MERCHANT_NAME': None,
            'PAYMENT_REPORT_VOUCHER': None,
            'ICNAME': None,
            'GROSS_AMOUNT': None,
            'REFUND': None,
            'CHARGEBACK': None,
            'LATE_RETURN': None,
            'CHARGES': None,
            'SERVICETAX': None,
            'SURCHARGE': None,
            'TDS': None,
            'NET_AMOUNT': None,

        }

        numericColumns = {
            'GROSS_AMOUNT': 'isnumeric',
            'REFUND': 'isnumeric',
            'CHARGEBACK': 'isnumeric',
            'LATE_RETURN': 'isnumeric',
            'CHARGES': 'isnumeric',
            'SERVICETAX': 'isnumeric',
            'SURCHARGE': 'isnumeric',
            'TDS': 'isnumeric',
        }

        dfObject = CheckValidity(pv_collection_data)

        exec_status = dfObject.removeDuplicateSelf(
            duplicate_list, "Duplicate PV No. Found", True)
        if exec_status != True:
            raise Exception(exec_status)

        exec_status = dfObject.checkValue(valuesToBeChecked)
        if exec_status != True:
            raise Exception(exec_status)

        exec_status = dfObject.checkValue(numericColumns)
        if exec_status != True:
            raise Exception(exec_status)

        data_csv = dfObject.getValidDf().drop(
            ['REMARK', 'BUSINESS_LOGIC_VALID'], errors='ignore', axis=1)

        # Insert feeback in to S3
        buf = io.BytesIO()
        feedback_df = dfObject.getInvalidDf().drop(
            ['UPLOAD_TASK_ID', 'UPDATED_USERID', 'UPDATED_DATE',  'BUSINESS_LOGIC_VALID'], errors='ignore', axis=1)
        feedback_df.to_csv(buf, encoding='utf-8', index=False)

        buf.seek(0)
        s3.put_object(Body=buf, Bucket='sdg-01',
                      Key=s3_feedback_path + str(upload_log_id) + "/Feedback.csv")

        load_file_path = file_path + "/" + file_name

        data_csv.to_csv(load_file_path, encoding='utf-8', header=True,
                        doublequote=True, sep=',', index=False, na_rep='\\N', quotechar='"')
        if data_csv.shape[0] != 0:
            with conn.cursor() as cur:
                load_sql = "LOAD DATA LOCAL INFILE '" + load_file_path + \
                    "' INTO TABLE PMFBY.COLLECTION_PV FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES;"
                print(load_sql)
                cur.execute(load_sql)
                conn.commit()

        os.remove(load_file_path)

        no_of_records_success = data_csv.shape[0]
        no_of_records_failed = (no_of_records_uploaded-no_of_records_success)

        # Update Feedback Available
        with conn.cursor() as cur:
            query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 1, PROCESS_END_TIME=now(), FEEDBACK_FILE = 'Feedback.csv', SUCCESS = "+str(no_of_records_success)+", FAILED = " + \
                str(no_of_records_failed) + \
                "  where id = "+str(upload_log_id)+";"
            print("Query: ", query)
            cur.execute(query)
            conn.commit()

        pool.release(conn)
        gc.collect()
        print("2", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return

    except Exception as e:
        print("Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        e_str = str(e).replace('"', '\\"')
        e_str = e_str.replace("'", "\\'")
        with conn.cursor() as cur:
            query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Something went wrong', EXCEPTION = '" + \
                str(e_str)+"' where id = "+str(upload_log_id)+";"
            # print("Query: ", query)
            cur.execute(query)
            conn.commit()

        gc.collect()
        return


def tid_collection_upload_thread(user_id, configid, upload_log_id, season, year, state, state_code):
    global tid_coll_row, pool
    try:
        print("1", upload_log_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print(pool)
        try:
            conn = pool.get_conn()
            with conn.cursor() as cur:
                query = "select now()"
                cur.execute(query)
                n = cur.fetchone()
        except:
            print(upload_log_id, 'Trying to reconnect')
            pool.init()
            conn = pool.get_conn()
        print(conn)

        with conn.cursor() as cur:
            query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE=0, PROCESS_START_TIME=now() where ID=" + \
                str(upload_log_id)
            print(upload_log_id, query)
            cur.execute(query)
            conn.commit()

        with conn.cursor() as cur:
            query = "select FILETYPE from PMFBY.FILEUPLOAD_LOGS where ID=" + \
                str(upload_log_id)
            print(upload_log_id, query)
            cur.execute(query)
            result = cur.fetchone()
        filetype = result['FILETYPE']
        if filetype not in ['excel', 'csv']:
            with conn.cursor() as cur:
                query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Invalid file type' where id = " + \
                    str(upload_log_id)+";"
                print("Query: ", query)
                cur.execute(query)
                conn.commit()
            pool.release(conn)
            gc.collect()
            return

        time_now = datetime.now()
        time_get = time_now.strftime("%Y%m%d%H%M%S")
        updated_date_time = time_now.strftime("%Y-%m-%d %H:%M:%S")
        file_name = 'tid_coll'+str(user_id)+"_" + \
            str(configid)+"_"+str(time_get)+".csv"

        tid_collection_data = pmfby.read_files_from_s3(upload_log_id, filetype)
        if type(tid_collection_data) is dict:
            with conn.cursor() as cur:
                e_str = str(tid_collection_data["error"]).replace('"', '\\"')
                e_str = e_str.replace("'", "\\'")
                query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK='Something went wrong', EXCEPTION = 'Error while reading file. " + e_str + "' where id = " + \
                    str(upload_log_id)+";"
                print("Query: ", query)
                cur.execute(query)
                conn.commit()
            pool.release(conn)
            gc.collect()
            return

        elif tid_collection_data.shape[0] == 0:
            with conn.cursor() as cur:
                query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Upload file(s) is/are empty' where id = " + \
                    str(upload_log_id)+";"
                print("Query: ", query)
                cur.execute(query)
                conn.commit()
            pool.release(conn)
            gc.collect()
            return

        print(upload_log_id, tid_collection_data.shape[0])

        ##### Checking if headers are matching #####

        columns = tid_coll_row
        headers = list(tid_collection_data.columns.values)
        headers.remove('FILENAME')

        try:
            if(headers != columns):
                with conn.cursor() as cur:
                    query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Headers do not match' where id = " + \
                        str(upload_log_id)+";"
                    print("Query: ", query)
                    cur.execute(query)
                    conn.commit()
                pool.release(conn)
                gc.collect()
                return
        except Exception as e:
            print("Exception Occurred")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print("Exception: ", e)
            e_str = str(e).replace('"', '\\"')
            e_str = e_str.replace("'", "\\'")
            with conn.cursor() as cur:
                query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK='Something went wrong', EXCEPTION = '" + \
                    str(e_str)+"' where id = "+str(upload_log_id)+";"
                print("Query: ", query)
                cur.execute(query)
                conn.commit()
            pool.release(conn)
            gc.collect()
            return

        dictionary = {'ID': 'ID', 'BILLERNAME': 'BILLER_NAME', 'DEBITTYPE': 'DEBIT_TYPE', 'NARRATION': 'NARRATION', 'PAYMODE': 'PAY_MODE', 'PRODUCTCODE': 'PRODUCT_CODE', 'BDREFNO': 'BD_REF_NO', 'OURID': 'OUR_ID', 'REF1': 'REF1', 'REF2': 'REF2', 'REF3': 'REF3', 'REF4': 'REF4',
                      'CREATEDON': 'CREATED_ON', 'GROSSAMOUNT': 'GROSS_AMOUNT', 'CHARGES': 'CHARGES', 'SERVICETAX': 'SERVICE_TAX', 'SURCHARGE': 'SURCHARGE', 'TDS': 'TDS', 'NETAMOUNT': 'NET_AMOUNT', 'FILENAME': 'FILENAME', 'SCHEME': 'SCHEME', 'SEASON': 'SEASON', 'YEAR': 'YEAR',
                      'STATE': 'STATE', 'SOURCE': 'SOURCE', 'POLICYID': 'POLICY_ID', 'BANKNAME': 'BANK_NAME', 'BRANCHCODE': 'BRANCH_CODE', 'BRANCHID': 'BRANCH_ID', 'BRANCHNAME': 'BRANCH_NAME', 'IFSC': 'IFSC', 'REFUND': 'REFUND', 'DUPLICATE': 'DUPLICATE'}

        tid_collection_data.rename(columns=dictionary, inplace=True)
        # tid_collection_data = tid_collection_data.drop()

        no_of_records_uploaded = tid_collection_data.shape[0]

        tid_collection_data['CONFIGID'] = configid

        tid_collection_data['UPLOAD_TASK_ID'] = upload_log_id

        tid_collection_data['UPDATED_USERID'] = user_id

        tid_collection_data['UPDATED_DATE'] = updated_date_time

        tid_collection_data = tid_collection_data.apply(
            lambda x: x.astype(str).str.upper().str.strip())

        tid_collection_data = tid_collection_data.replace('', None)

        duplicate_list = ['BD_REF_NO']
        # de_duplicate_list = []

        dfObject = CheckValidity(tid_collection_data)

        exec_status = dfObject.removeDuplicateFromDf(
            duplicate_list, "Duplicate BD Ref No. found")
        if exec_status != True:
            raise Exception(exec_status)

        exec_status = dfObject.removeDuplicateSelf(
            duplicate_list, "Duplicate BD Ref No. found in file", True)
        if exec_status != True:
            raise Exception(exec_status)

        valuesToBeChecked = {
            'ID': None,
            'BILLER_NAME': None,
            'DEBIT_TYPE': None,
            'NARRATION': None,
            'PAY_MODE': None,
            'PRODUCT_CODE': None,
            'BD_REF_NO': None,
            'INDEMNITY': None,
            'OUR_ID': None,
            'BANK_NAME': None,
            'BRANCH_CODE': None,
            'BRANCH_ID': None,
            'BRANCH_NAME': None,
            'IFSC': None,
        }

        numericColumns = {
            'GROSS_AMOUNT': 'isnumeric',
            'CHARGES': 'isnumeric',
            'SERVICE_TAX': 'isnumeric',
            'SURCHARGE': 'isnumeric',
            'TDS': 'isnumeric',
            'NET_AMOUNT': 'isnumeric',
            'REFUND': 'isnumeric',
        }

        exec_status = dfObject.checkValue(valuesToBeChecked)
        if exec_status != True:
            raise Exception(exec_status)

        exec_status = dfObject.checkValue(numericColumns)
        if exec_status != True:
            raise Exception(exec_status)

        data_csv = dfObject.getValidDf().drop(
            ['REMARK', 'BUSINESS_LOGIC_VALID'], axis=1)

        # # ***********Adding remaining columns of PMFBY****************
        # col = ['PMFBY_NOTIF_ID', 'CROP_NAME', 'CROP_CODE', 'SSSYID', 'STATE_CODE', 'L4_TERM', 'L5_TERM', 'L6_NAME', 'L6_NAME_CODE', 'L6_TERM', 'L7_NAME', 'L7_NAME_CODE', 'INSURANCE_UNIT_NAME',
        #        'INSURANCE_COMPANY_CODE', 'GOI_SHARE_PCT', 'CUT_OFF_DATE', 'THRESHOLD_YIELD', 'DRIAGE_FACTOR', 'EXPECTED_SUM_INSURED', 'NO_OF_CCE_REQD', 'CCE_AREA', 'EXPECTED_PREMIUM', 'GOVT_THRESHOLD_YIELD', 'GOVT_ACTUAL_YIELD']

        # data_csv = pd.concat([data_csv, pd.DataFrame(columns=col)])

        # data_csv = data_csv.reindex(['PMFBY_NOTIF_ID', 'CROP_NAME', 'CROP_CODE', 'SSSYID', 'STATE', 'STATE_CODE', 'L3_NAME_CODE', 'L3_NAME', 'L4_NAME', 'L4_NAME_CODE', 'L4_TERM', 'L5_NAME', 'L5_NAME_CODE', 'L5_TERM', 'L6_NAME', 'L6_NAME_CODE', 'L6_TERM', 'L7_NAME', 'L7_NAME_CODE', 'INSURANCE_UNIT_NAME',
        #                              'INSURANCE_COMPANY_CODE', 'INSURANCE_COMPANY_NAME', 'PREMIUM_RATE_PCT', 'FARMER_SHARE_PCT', 'GOI_SHARE_PCT', 'STATE_SHARE_PCT', 'INDEMNITY', 'SUM_INSURED', 'CUT_OFF_DATE', 'NOTIFIED_IU_NAME', 'THRESHOLD_YIELD', 'DRIAGE_FACTOR', 'EXPECTED_SUM_INSURED', 'NO_OF_CCE_REQD', 'CCE_AREA', 'EXPECTED_PREMIUM', 'GOVT_THRESHOLD_YIELD', 'GOVT_ACTUAL_YIELD', 'CONFIGID',
        #                              'UPLOAD_TASK_ID', 'UPDATED_USERID', 'UPDATED_DATE', 'NOTIFICATION_STATUS', 'LAST_DENOTIFY_DATE'], axis=1)

        # Insert feeback in to S3
        buf = io.BytesIO()
        feedback_df = dfObject.getInvalidDf().drop(['CONFIGID', 'UPLOAD_TASK_ID', 'UPDATED_USERID',
                                                    'UPDATED_DATE', 'BUSINESS_LOGIC_VALID'], axis=1)
        feedback_df.to_csv(buf, encoding='utf-8', index=False)

        buf.seek(0)
        s3.put_object(Body=buf, Bucket='sdg-01',
                      Key=s3_feedback_path + str(upload_log_id) + "/Feedback.csv")

        print("data_csv-------------", data_csv.shape[0])
        if data_csv.shape[0] != 0:

            data_csv.insert(0, "NOTIFICATION_ID", None)
            load_file_path = file_path + "/" + file_name

            data_csv.to_csv(load_file_path, encoding='utf-8', header=True,
                            doublequote=True, sep=',', index=False, na_rep='\\N', quotechar='"')

            with conn.cursor() as cur:
                load_sql = "LOAD DATA LOCAL INFILE '" + load_file_path + \
                    "' INTO TABLE PMFBY.COLLECTION_TID FIELDS TERMINATED BY ',' ENCLOSED BY '\"');"
                print(load_sql)
                cur.execute(load_sql)
                conn.commit()

            os.remove(load_file_path)

        no_of_records_success = data_csv.shape[0]
        no_of_records_failed = (no_of_records_uploaded-no_of_records_success)

        # Update Feedback Available
        with conn.cursor() as cur:
            query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 1, PROCESS_END_TIME=now(), FEEDBACK_FILE = 'Feedback.csv', SUCCESS = "+str(no_of_records_success)+", FAILED = " + \
                str(no_of_records_failed) + \
                "  where id = "+str(upload_log_id)+";"
            print("Query: ", query)
            cur.execute(query)
            conn.commit()

        pool.release(conn)
        gc.collect()
        print("2", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return

    except Exception as e:
        print("Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        e_str = str(e).replace('"', '\\"')
        e_str = e_str.replace("'", "\\'")
        with conn.cursor() as cur:
            query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK='Something went wrong', EXCEPTION = '" + \
                str(e_str)+"' where id = "+str(upload_log_id)+";"
            # print("Query: ", query)
            cur.execute(query)
            conn.commit()

        gc.collect()
        return


def unknown_process_thread(upload_log_id):
    try:

        global pool
        try:
            conn = pool.get_conn()
            with conn.cursor() as cur:
                query = "select now()"
                cur.execute(query)
                n = cur.fetchone()
        except:
            print('Trying to reconnect')
            pool.init()
            conn = pool.get_conn()

        # *****database entry*****
        with conn.cursor() as cur:
            query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE=2, PROCESS_START_TIME=now(), PROCESS_END_TIME=now(), REMARK='Unknown process', EXCEPTION='Unknown process' where ID=" + \
                str(upload_log_id)
            print(upload_log_id, query)
            cur.execute(query)
            conn.commit()

    except Exception as e:
        print("Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        e_str = str(e).replace('"', '\\"')
        e_str = e_str.replace("'", "\\'")
        with conn.cursor() as cur:
            query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Something went wrong', EXCEPTION = '" + \
                str(e_str)+"' where id = "+str(upload_log_id)+";"
            # print("Query: ", query)
            cur.execute(query)
            conn.commit()
        pool.release(conn)
        gc.collect()

def get_collection_thread():
    conn = pool.get_conn()
    try:
        entry_type='BOOKING_TAGID'
        query=f'select TID,POLICY_ID,PARTY_ID,NET_AMOUNT,REMAINING_AMOUNT,CONFIGID,SCHEME,SEASON,YEAR,STATE,SOURCE FROM PMFBY.COLLECTION_TID where SOURCE IN(1,3,7) AND REMAINING_AMOUNT > 0 AND REFUND_ID IS NULL;'
        TID_dataframe = pd.read_sql(query, connection_string)
        policy_id_list=TID_dataframe[['PARTY_ID','CONFIGID']].dropna().values.tolist()
        print("policy_id_list=",policy_id_list)
        '''we are indicating the locations where the parameter values will be inserted 
        when executing the query for this we are using ({configid_placeholders}) and ({partyid_placeholders})'''
        query2 = '''
            SELECT pro.PROPOSAL_ID, pro.CONFIGID AS proposal_config, pro.FARMER_SHAREOF_PREMIUM AS COLLECTION_AMOUNT, pro.PARTY_ID,
            config.STATE, config.YEAR, config.SEASON, config.PRODUCT, state_master.STATE_CODE, state_master.STATE_NAME
            FROM PMFBY.PROPOSAL AS pro
            LEFT JOIN PMFBY.PRODUCT_CONFIG AS config ON config.CONFIGID = pro.CONFIGID
            INNER JOIN PMFBY.STATE_MST AS state_master ON state_master.STATE_NAME = config.STATE
            WHERE pro.CONFIGID IN ({configid_placeholders}) AND pro.PARTY_ID IN ({partyid_placeholders})
        '''

        # Create placeholders for CONFIGID and PARTY_ID based on the length of policy_id_list
        configid_placeholders = ', '.join(['%s'] * len(policy_id_list))
        partyid_placeholders = ', '.join(['%s'] * len(policy_id_list))

        # Create separate lists for CONFIGID and PARTY_ID
        configid_list = [item[1] for item in policy_id_list]
        partyid_list = [item[0] for item in policy_id_list]

        # Format the query by replacing the placeholders with the actual values
        formatted_query = query2.format(configid_placeholders=configid_placeholders, partyid_placeholders=partyid_placeholders)

        # Flatten the policy_id_list
        params = configid_list + partyid_list

        # Execute the query and pass the parameters as a single flat list
        proposal_dataframe = pd.read_sql_query(formatted_query, connection_string, params=params)
        x=proposal_collection_entry(conn,proposal_dataframe,TID_dataframe,entry_type)
    
    except Exception as e:
        print("Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        gc.collect()

def get_subsidy_thread():
    conn = pool.get_conn()
    try:
        query=f'select TID,POLICY_ID,PARTY_ID,NET_AMOUNT,REMAINING_AMOUNT,SCHEME,SEASON,YEAR,STATE,SOURCE FROM PMFBY.COLLECTION_TID where SOURCE IN(1,3,7) AND REMAINING_AMOUNT > 0 ;'
        TID_dataframe = pd.read_sql(query, connection_string)
        policy_id_list=TID_dataframe['PARTY_ID'].dropna().to_list()
        query2='select pro.PROPOSAL_ID,pro.CONFIGID as proposal_config ,config.CONFIGID as con_config ,pro.GOI_SHAREOF_PREMIUM AS COLLECTION_AMOUNT,pro.PARTY_ID,config.STATE,config.YEAR,config.SEASON,config.PRODUCT,state_master.STATE_CODE,state_master.STATE_NAME  from PMFBY.PROPOSAL as pro left join  PMFBY.PRODUCT_CONFIG as config on config.CONFIGID=pro.CONFIGID inner join PMFBY.STATE_MST as state_master on state_master.STATE_NAME=config.STATE  where pro.PARTY_ID in (%s)'
        placeholders = ', '.join(['%s' for _ in policy_id_list])
        formatted_query = query2 % placeholders
        proposal_dataframe = pd.read_sql_query(formatted_query, connection_string, params=policy_id_list)
        x=proposal_collection_entry(conn,proposal_dataframe,TID_dataframe,'SUBSIDY_TAGID')
    
    except Exception as e:
        print("Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        gc.collect()

def get_rejected_collection(prposal_id_list:list):
    conn = pool.get_conn()
    try:
        # prposal_id_list = [(9611, 4), (9628, 4), (9629, 4), (9669, 4)] # example for testing
        query1 = 'SELECT PROPOSAL_ID,FARMER_SHAREOF_PREMIUM, PARTY_ID, CONFIGID, BOOKING_TAGID FROM PMFBY.PROPOSAL WHERE PROPOSAL_ID IN ({}) AND CONFIGID = %s;'.format(', '.join(['%s'] * len(prposal_id_list)))
        params = [proposal_id[0] for proposal_id in prposal_id_list]
        params.append(prposal_id_list[0][1])  # Append CONFIGID from the first tuple
        proposal_dataframe = pd.read_sql_query(query1, connection_string, params=params) 
        booking_id=proposal_dataframe['BOOKING_TAGID'].dropna().unique().tolist()
        query2 = 'SELECT * FROM PMFBY.TID_MAPING WHERE TAG_ID=%s;'
        tag_dataframe = pd.read_sql_query(query2, connection_string, params=booking_id)
        tag_id_dataframe=tag_dataframe['TAG_ID'].unique()
        tid_mapping=[]
        update_tid_remaining_amount=[]
        for proposalid in prposal_id_list:
            propl_id=proposal_dataframe[proposal_dataframe['PROPOSAL_ID']==proposalid[0]]
            for tid in tag_id_dataframe:
                orderby_data = tag_dataframe[tag_dataframe['TAG_ID'] == int(tid)].sort_values(by=['AMOUNT'], ascending=False)
                amount=float(propl_id['FARMER_SHAREOF_PREMIUM'])
                for _,value in orderby_data.iterrows():
                    amount=tag_dataframe[tag_dataframe['TID']==int(value['TID'])].sort_values(by=['AMOUNT'], ascending=False)['AMOUNT'].sum()
                    print("amount",amount)
                    if amount <= float(propl_id['FARMER_SHAREOF_PREMIUM']) and amount > 0:
                        amount=float(propl_id['FARMER_SHAREOF_PREMIUM'])-amount
                        tid_mapping.append((int(value['TID']),proposalid[0],int(value['TAG_ID']),float(-value['AMOUNT'])))
                        update_tid_remaining_amount.append((float(value['AMOUNT']),int(value['TID'])))
                    
                    elif amount >= float(propl_id['FARMER_SHAREOF_PREMIUM']) and amount > 0:
                        amount=amount-float(propl_id['FARMER_SHAREOF_PREMIUM'])-amount
                        tid_mapping.append((int(value['TID']),proposalid[0],int(value['TAG_ID']),amount))
                        update_tid_remaining_amount.append((-amount,int(value['TID'])))
                        break
                    else:
                        break 
            with conn.cursor() as cur: 

                query5 = "INSERT INTO `PMFBY`.`TID_MAPING`(`TID`,`POLICY_ID`,`TAG_ID`,`AMOUNT`)VALUES(%s,%s,%s,%s);"
                cur.executemany(query5,tid_mapping)
                conn.commit()  

                query9 = "UPDATE PMFBY.COLLECTION_TID SET REMAINING_AMOUNT = REMAINING_AMOUNT + %s WHERE TID = %s "
                cur.executemany(query9, update_tid_remaining_amount)
                conn.commit()  

                query9 = "UPDATE PMFBY.PROPOSAL SET BOOKING_TAGID = NULL WHERE PROPOSAL_ID = %s "
                cur.execute(query9, proposalid[0])
                conn.commit()
            tid_mapping=[]
            update_tid_remaining_amount=[]  

    except Exception as e:
        print("Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        gc.collect()        


def get_collection_unTag(Tid_list:list,configid:int):
    conn = pool.get_conn()
    try:
        Tid_list=[29]
        configid=4
        # fetch Tag_id,tid and sum based on tid 
        query='select TAG_ID ,TID,sum(AMOUNT) AS AMOUNT FROM PMFBY.TID_MAPING WHERE TAG_ID IN(SELECT TAG_ID FROM TID_MAPING WHERE TID in (%s)) GROUP BY TID,TAG_ID  having AMOUNT>0 ORDER BY AMOUNT DESC;'
        placeholders = ', '.join(['%s' for _ in Tid_list])
        formatted_query = query % placeholders
        tag_amount_dataframe = pd.read_sql_query(formatted_query, connection_string, params=Tid_list)
        tag_id=tag_amount_dataframe['TAG_ID'].unique().tolist()

        # fetch prposal based on tag_id fetch from maping dataframe(tag_amount_dataframe)
        query1='select PROPOSAL_ID,FARMER_SHAREOF_PREMIUM,BOOKING_TAGID FROM PMFBY.PROPOSAL WHERE BOOKING_TAGID IN ({}) and CONFIGID=%s ;'
        placeholders1 = ', '.join(['%s'] * len(tag_id))
        formatted_query1 = query1.format(placeholders1)
        params = tuple(tag_id) + (configid,)
        proposal_dataframe = pd.read_sql_query(formatted_query1, connection_string, params=params)
        unTag_propodalId=[]
        tid_mapping=[]
        update_tid_remaining_amount=[]
        for tag in tag_id:    # loop for tag and untag Booking id in proposal
            map_dataframe=tag_amount_dataframe[tag_amount_dataframe['TAG_ID']==int(tag)]
            if map_dataframe['TID'].values[0] in Tid_list:   #check first element in dataframe id its the tid that are to be untag than untag all the TagId in proposal 
                tid=map_dataframe.values[0]
                tid_mapping.append((tid[1].astype(int),tag,-tid[2]))
                bookingtag=proposal_dataframe.loc[proposal_dataframe['BOOKING_TAGID'] == int(tag), 'PROPOSAL_ID'].values.tolist()
                unTag_propodalId.append(bookingtag)
                update_tid_remaining_amount.append((tid[2],tid[1].astype(int)))
            else:
                bookingtag = map_dataframe[map_dataframe['TID'].isin(Tid_list)] #to filter the dataframe using a list of values is to use the isin function
                tid_sum_amount=bookingtag['AMOUNT'].sum()                
                tag_proposal=proposal_dataframe[proposal_dataframe['BOOKING_TAGID']==int(tag)].sort_values('FARMER_SHAREOF_PREMIUM',ascending=True)
                for _,pro_value in tag_proposal.iterrows(): 
                
                    if pro_value['FARMER_SHAREOF_PREMIUM'] <= tid_sum_amount:
                        tid_sum_amount=tid_sum_amount-pro_value['FARMER_SHAREOF_PREMIUM']
                        unTag_propodalId.append(pro_value['PROPOSAL_ID'].astype(int))

                    elif pro_value['FARMER_SHAREOF_PREMIUM'] >= tid_sum_amount and tid_sum_amount > 0  :
                        tid_sum_amount=tid_sum_amount-pro_value['FARMER_SHAREOF_PREMIUM']
                        unTag_propodalId.append(pro_value['PROPOSAL_ID'].astype(int))
                        tid_mapping.append([(i['TID'].astype(int),i['TAG_ID'].astype(int),-float(i['AMOUNT'])) for _,i in bookingtag.iterrows()])
                        tid=map_dataframe.values[0]
                        tid_mapping.append((tid[1].astype(int),tag,float(tid_sum_amount)))
                        update_tid_remaining_amount.append((tid[2],tid[1].astype(int)))
                        update_tid_remaining_amount.append((-float(tid_sum_amount),tid[1].astype(int)))
                        
                    else:
                        pass  
                    
        with conn.cursor() as cur: 

            query3 = "UPDATE PMFBY.PROPOSAL SET BOOKING_TAGID = NULL WHERE PROPOSAL_ID IN (%s);"
            placeholders2 = ', '.join(['%s'] * len(unTag_propodalId))
            formatted_query2 = query3 % placeholders1 
            cur.executemany(formatted_query2, unTag_propodalId)
         

            query5 = "INSERT INTO `PMFBY`.`TID_MAPING`(`TID`,`TAG_ID`,`AMOUNT`)VALUES(%s,%s,%s);"
            cur.executemany(query5, )
            

            query9 = "UPDATE PMFBY.COLLECTION_TID SET REMAINING_AMOUNT = REMAINING_AMOUNT + %s WHERE TID = %s "
            cur.executemany(query9, update_tid_remaining_amount)
            conn.commit()  

            
            tid_mapping=[]
            update_tid_remaining_amount=[]        
        print("unTag_propodalId==",unTag_propodalId) 

    except Exception as e:
        print("Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        gc.collect()           

def get_partywise_collection_refund(party_id_list:list,configid:int):
    conn = pool.get_conn()
    cur_date = datetime.now()
    try:
        # party_id_list=['133/129826/RABI/2022','133/129838/RABI/2022']   # testing example
        # configid=4                                                      # testing example
        query='SELECT * FROM PMFBY.COLLECTION_TID where PARTY_ID in({}) and CONFIGID=%s AND REMAINING_AMOUNT > 0 ;'
        placeholders1 = ', '.join(['%s'] * len(party_id_list))
        formatted_query1 = query.format(placeholders1)
        params = tuple(party_id_list) + (configid,)
        tag_amount_dataframe = pd.read_sql_query(formatted_query1,connection_string,params=params)
        partywise_tag_amount_dataframe=tag_amount_dataframe.groupby('PARTY_ID').agg({'REMAINING_AMOUNT':'sum'})

        for key,value in partywise_tag_amount_dataframe.iterrows():
            partywise_data=tag_amount_dataframe.loc[tag_amount_dataframe['PARTY_ID'] == key, 'TID'].to_list()
            with conn.cursor() as cur:
                query8 = "INSERT INTO `PMFBY`.`COLLECTION_TAGGING`(`DATE`,`TAG_AMOUNT`,`CONFIGID`,`TYPE`)VALUES(%s,%s,%s,%s);"
                parm=[str(cur_date),value['REMAINING_AMOUNT'],int(configid),'REFUND']
                cur.execute(query8,parm)
                conn.commit()
                tag_id = cur.lastrowid
                
                query9 = "UPDATE PMFBY.COLLECTION_TID SET REFUND_ID = %s WHERE TID IN ({}) "
                placeholders = ', '.join(['%s'] * len(partywise_data))
                formatted_query = query9.format(placeholders)
                Params1 = (tag_id,) + tuple(partywise_data)
                print(formatted_query)
                cur.execute(formatted_query, Params1)
                conn.commit()
    except Exception as e:
        print("Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        gc.collect()     



def proposal_collection_entry(conn:pymysql.connections.Connection,proposal_dataframe:pd.DataFrame,TID_dataframe:pd.DataFrame,entry_type:str='BOOKING_TAGID'):
    cur_date = datetime.now()
    groupby_collection=TID_dataframe.groupby(['PARTY_ID','SCHEME','SEASON','YEAR','STATE','CONFIGID']).agg({'REMAINING_AMOUNT': 'sum'})
    print("groupby_collection==",groupby_collection)
    for filter_value, function_value in groupby_collection.iterrows():
    # state_code = collection_key[4]
    # state_name = proposal_dataframe.loc[proposal_dataframe['STATE_CODE'] == state_code, 'STATE_NAME'].values[0]
        config = filter_value[5]
        ch_party_id=filter_value[0]
        filtered_df = proposal_dataframe[(proposal_dataframe['PARTY_ID'].astype(str) == str(ch_party_id))
            & (proposal_dataframe['proposal_config'] == int(config) )].sort_values('COLLECTION_AMOUNT',ascending=[True])
        amount_ch=float(function_value['REMAINING_AMOUNT'])
        ch_list=[]
        print("filtered_df===",filtered_df)
        print("amount_ch==",amount_ch)
        for _, proposal_value in filtered_df.iterrows():
            if float(proposal_value['COLLECTION_AMOUNT']) < amount_ch:
                amount_ch=amount_ch-float(proposal_value['COLLECTION_AMOUNT'])
                ch_list.append(proposal_value['PROPOSAL_ID'])
            else:
                print("final amount",amount_ch)
                break
        tagamount=float(function_value['REMAINING_AMOUNT'])-amount_ch
        # config_unique = filtered_df['proposal_config'].unique()
        print("ch_list==",ch_list)
        with conn.cursor() as cur:
            query8 = "INSERT INTO `PMFBY`.`COLLECTION_TAGGING`(`DATE`,`TAG_AMOUNT`,`CONFIGID`,`TYPE`)VALUES(%s,%s,%s,%s);"
            parm=[str(cur_date),tagamount,int(config),'BOOKING']
            cur.execute(query8,parm)
            conn.commit()
            tag_id = cur.lastrowid
            
            query8 = "UPDATE PMFBY.PROPOSAL SET {} = %s WHERE CONFIGID = %s AND PROPOSAL_ID IN ({})".format(entry_type, ','.join(['%s'] * len(ch_list)))
            parameters = [(tag_id, config, proposal_id) for proposal_id in ch_list]
            cur.executemany(query8, parameters)
            conn.commit()

        tid_update=TID_dataframe[(TID_dataframe['PARTY_ID'] == str(ch_party_id)) & (TID_dataframe['CONFIGID'] == int(config))].sort_values(by=['REMAINING_AMOUNT'], ascending=True)  
        update_tid_remaining_amount=[]
        tid_mapping=[]
        tagged_amount_balance=tagamount
        for _, Tid_value in tid_update.iterrows(): 
            print("Tid_valueREMAINING_AMOUNT==",Tid_value['REMAINING_AMOUNT'])
            
            print("remaining_amount",tagged_amount_balance)
            if tagged_amount_balance > 0: 
                if Tid_value['REMAINING_AMOUNT'] <= tagged_amount_balance:
                    tagged_amount_balance=tagged_amount_balance-Tid_value['REMAINING_AMOUNT']
                    update_tid_remaining_amount.append((0,Tid_value['TID']))
                    tid_mapping.append((Tid_value['TID'],tag_id,Tid_value['REMAINING_AMOUNT']))
                else:
                    update_tid_remaining_amount.append((Tid_value['REMAINING_AMOUNT']-tagged_amount_balance,Tid_value['TID']))
                    tid_mapping.append((Tid_value['TID'],tag_id,tagged_amount_balance))
                    tagged_amount_balance=0
            else:
                break 
        print("tid_mapping==",tid_mapping)
        with conn.cursor() as cur: 
            query9 = "UPDATE PMFBY.COLLECTION_TID SET REMAINING_AMOUNT = %s WHERE TID = %s "
            cur.executemany(query9, update_tid_remaining_amount)
            conn.commit()  

            query5 = "INSERT INTO `PMFBY`.`TID_MAPING`(`TID`,`TAG_ID`,`AMOUNT`)VALUES(%s,%s,%s);"
            cur.executemany(query5, tid_mapping)
            conn.commit()         