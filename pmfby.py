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
# from custom_thread_accounts import SARUS_accounting_thread
from dfvaliditycheck import CheckValidity

empRow = None
# import aiohttp

pmfby = Blueprint('pmfby', __name__)
LOG = logging.getLogger(__name__)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)


pool = Pool(host=endpoint, port=3306, user=username,
            password=password, db=database_name, local_infile=1)
pool.init()

# live_pool = Pool(host=live_endpoint, port=3306, user=live_username,
#             password=live_password, db=live_database_name, local_infile=1)
# pool.init()

notif_row = ['CROP NOTIFICATIONS ID*', 'CROP NAME*', 'CROP CODE*', 'SSSY ID*', 'STATE NAME*',
             'STATE CODE*', 'DISTRICT CODE*', 'DISTRICT NAME*', 'LEVEL 4 NAME*', 'LEVEL 4 CODE*', 'LEVEL 4*', 'LEVEL 5 NAME*',
             'LEVEL 5 CODE*', 'LEVEL 5*', 'LEVEL 6 NAME*', 'LEVEL 6 CODE*', 'LEVEL 6*', 'VILLAGE NAME*', 'VILLAGE CODE*',
             'INSURANCE UNIT NAME*', 'INSURANCE COMPANY CODE*', 'INSURANCE COMPANY NAME*', 'PREMIUM RATE*', 'FARMER SHARE*',
             'GOI SHARE*', 'STATE SHARE*', 'INDEMNITY LEVEL*', 'SUM INSURED*', 'CUT OF DATE*', 'NOTIFIED IU NAME*',
             'GOVT THRESHOLD YIELD (KG/HECTARE)*', 'EXPECTED DRIAGE FACTOR (%)*', 'EXPECTED SUM INSURED (RS.)*', 'NO. OF CCE REQD.*', 'CCE AREA (SQ. M)*']

uw_row = ["SCHEME NAME",	"YEAR",	"SEASON",	"STATE NAME",	"APPLICATION ID",	"FARMER ID",	"FARMER NAME",	"FARMER ID TYPE",	"FARMER ID NUMBER",	"RELATIVE NAME",	"RELATIVE'S RELATION",	"FARMER MOBILE",	"AGE",	"GENDER",	"COMMUNITY",	"ADDRESS",	"STATE OF RESIDENCE",	"STATE CODE (RESIDENCE)",	"DISTRICT OF RESIDENCE",	"DISTRICT CODE (RESIDENCE)",	"SUB DISTRICT NAME OF RESIDENCE",	"SUB DISTRICT CODE (RESIDENCE)",	"VILLAGE OF RESIDENCE",	"VILLAGE CODE(RESIDENCE)",	"PINCODE",	"BANK NAME",	"BANK ID",	"BRANCH NAME",	"BRANCH ID",	"IFSC",	"FARMER TYPE",	"ACCOUNT OWNERSHIP TYPE",	"ACCOUNT NUMBER",	"FARMER CATEGORY",	"NATURE OF FARMER",	"FARMER ACCOUNT TYPE",	"INSURANCE COMPANY NAME",	"INUSRANCE COMPANY CODE",	"CROP NAME",	"CROP CODE",	"CROP STATE NAME",	"CROP STATE CODE",	"CROP DISTRICT NAME",	"CROP DISTRICT CODE",
          "IU LEVEL",	"IU NAME",	"IU CODE",	"CROP VILLAGE NAME",	"CROP VILLAGE CODE",	"LAND SURVEY NUMBER",	"LAND SUBDIVISION NUMBER",	"AREA INSURED",	"FARMER SHARE",	"STATE SHARE",	"GOI SHARE",	"GROSS PREMIUM",	"SUM INSURED",	"MIX CROP",	"COMBINED AREA INSURED",	"PREMIUM DEBIT DATE",	"SOWING DATE",	"CUT OF DATE",	"INDEMNITY LEVEL",	"APPLICATION STATUS",	"APPLICATION SOURCE",	"UTR/TRANSACTION NUMBER",	"UTR/TRANSACTION AMOUNT",	"UTR/TRANSACTION DATE",	"UTR/TRANSACTION MODE",	"LEVEL4NAME",	"LEVEL4CODE",	"LEVEL5NAME",	"LEVEL5CODE",	"LEVEL6NAME",	"LEVEL6CODE",	"TIMESTAMP DATA GENERATION",	"CREATED BANK NAME",	"CREATED BANK CODE",	"CREATED BRANCH NAME",	"CREATED BRANCH CODE",	"CREATED USER NAME",	"CSC ID",	"APPLICATION CREATED DATE",	"LAND RECORD FARMER NAME",	"LAND RECORD AREA", "DUPLICATE"]

euw_row = ['APPLICATION ID*', 'FARMER ID', 'FARMER NAME', 'RELATIVE NAME', "RELATIVE'S RELATION", 'FARMER MOBILE', 'AGE', 'GENDER', 'COMMUNITY',
           'ADDRESS', 'STATE OF RESIDENCE', 'STATE CODE (RESIDENCE)', 'DISTRICT OF RESIDENCE', 'DISTRICT CODE (RESIDENCE)', 'SUB DISTRICT NAME OF RESIDENCE', 'SUB DISTRICT CODE (RESIDENCE)',
           'VILLAGE OF RESIDENCE', 'VILLAGE CODE (RESIDENCE)', 'PINCODE', 'BANK NAME', 'BANK ID', 'BRANCH NAME', 'BRANCH ID', 'IFSC', 'FARMER TYPE', 'ACCOUNT OWNERSHIP TYPE', 'ACCOUNT NUMBER',
           'FARMER CATEGORY', 'NATURE OF FARMER', 'FARMER ACCOUNT TYPE', 'CROP NAME', 'CROP CODE', 'LAND SURVEY NUMBER', 'LAND SUBDIVISION NUMBER', 'AREA INSURED', 'MIX CROP',
           'COMBINED AREA INSURED', 'PREMIUM DEBIT DATE', 'SOWING DATE', 'CUT OF DATE', 'APPLICATION SOURCE', 'UTR/TRANSACTION NUMBER', 'UTR/TRANSACTION AMOUNT', 'UTR/TRANSACTION DATE',
           'UTR/TRANSACTION MODE', 'CREATED BANK NAME', 'CREATED BANK CODE', 'CREATED BRANCH NAME', 'CREATED BRANCH CODE', 'CREATED USER NAME', 'CSC ID', 'APPLICATION CREATED DATE', 'LAND RECORD FARMER NAME', 'LAND RECORD AREA']

# pv_coll_row = ['SR NO', 'DEBIT TYPE*', 'PAY TYPE*', 'GROSS AMOUNT*', 'REFUND*', 'CHARGEBACK', 'LATE RETURN', 'CHARGES', 'SERVICETAX', 'SURCHARGE', 'TDS',	'NET AMOUNT*', 'PV NO*', 'FILE DATE*', 'MERCHANT NAME*', 'PAYMENT REPORT VOUCHER*',
#                'ICNAME', 'ID', 'SEASON', 'YEAR', 'REMARKS']

# tid_coll_row = ["ID", "BILLERNAME", "DEBITTYPE", "NARRATION", "PAYMODE", "PRODUCTCODE", "BDREFNO", "OURID", "REF1", "REF2", "REF3", "REF4",
#                 "CREATEDON", "GROSSAMOUNT",	"CHARGES", "SERVICETAX", "SURCHARGE", "TDS", "NETAMOUNT", "FILENAME", "SCHEME", "SEASON", "YEAR",
#                 "STATE", "SOURCE", "POLICYID", "BANKNAME", "BRANCHCODE", "BRANCHID", "BRANCHNAME", "IFSC", "REFUND", "DUPLICATE"]

bsb_notif_row_kharif = ["STATE NAME",	"DISTRICT NAME", "L&LR DCODE", "BLOCK NAME", "L&LR BCODE", "PANCHAYAT NAME", "ING GPCODE", "CROP",
                        "NOTIFICATION UNIT", "NOTIFICATION STATUS", "INSURANCE COMPANY", "CLUSTER", "SCALE OF FINANCE", "PREMIUM RATE", "PREMIUM/HA (RS.)", 'STATE SHARE\n%', 'FARMER SHARE \n%', "FARMER SHARE/HA (RS.)", "STATE SHARE/HA (RS.)", "INDEMNITY %"]

bsb_notif_row_rabi = ['STATE NAME', 'DISTRICT NAME', 'L&LR DCODE', 'BLOCK NAME', 'L&LR BCODE', 'PANCHAYAT NAME', 'ING GPCODE', 'CROP', 'NOTIFICATION UNIT', 'NOTIFICATION STATUS',
                      'INSURANCE COMPANY', 'CLUSTER', 'SCALE OF FINANCE', 'PREMIUM RATE (%)', 'PREMIUM/HA (RS.)', 'GOVT. \nSHARE %', 'FARMER \nSHARE %', 'FARMER SHARE/HA (RS.)', 'INDEMNITY %']

bsb_uw_row = ['YEAR', 'SEASON', 'APPLICATION ID', 'NAME OF FARMER', "FATHER'S NAME", 'EPIC NO.', 'AADHAR NO.', 'KB NO.', 'AGE', 'GENDER', 'CASTE', 'MOBILE', 'CROP', 'DISTRICT', 'BLOCK', 'GP', 'MOUZA', 'JL NO.', 'KHATIAN NO', 'PLOT NUMBER', 'INSURANCE UNIT', 'AREA INSURED IN ACRE.',
              'SUM INSURED', 'GROSS PREMIUM', 'FARMER CATEGORY', 'NATURE OF FARMER', 'FARMER NAME AS PER BANK', 'BANK NAME', 'BRANCH NAME', 'IFSC', 'BANK ACCOUNT NUMBER', 'APPLICATION TYPE', 'APPLICATION STATUS', 'APPLICATION SOURCE', 'CREATED USER NAME', 'CREATED USER TYPE', 'KB VALIDATED', 'DUPLICATE']

intimation_row = ['Govt Portal Application No.*', 'Docket ID/Loss Intimation No.', 'Intimated Land Survey No.*', 'Intimated Land Subsurvey No.',
                  'Intimated Crop*', 'Intimated Mobile No*', 'Crop Loss cover under Itimation*', 'Intimated Cause of Loss*', 'Intimated Date of Loss Event/s*', 'Date of Intimation at NCIP']


intimation_db_row = ['FARMER_APPLICATION_ID','LOSS_INTIMATION_NO','LAND_SURVEY_NUMBER','LAND_SUB_DIVISION_NUMBER',
'FARMER_CROP_NAME','FARMER_MOBILE','COVER_TYPE','PERIL','PERIL_DATE','CREATION_DATE_NCIP']  



@pmfby.errorhandler(404)
def not_found(e):
    return "ERROR"


@pmfby.route('/', methods=['GET'])
def home():
    return "Home Page"


@pmfby.route('/get_pmfby_notification', methods=['GET'])
def get_notification():
    try:
        print("Notif search - PMFBY")
        global pool
        print(pool)
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

        print(request.json)
        configid = request.json['configid']
        district = request.json['district']
        method = request.json['method']

        if method in ['PMFBY_Notification_View', 'RWBCIS_Notification_View']:
            col_name = 'Village'
            query = 'select NOTIFICATION_ID, L3_NAME, L4_NAME, L5_NAME, L6_NAME, L7_NAME as "Village", CROP_NAME, SUM_INSURED, PREMIUM_RATE_PCT, NOTIFICATION_STATUS from PMFBY.NOTIFICATION where configid=' + \
            str(configid)
        elif method == 'BSB_Notification_View':
            col_name = 'Panchayat'
            query = 'select NOTIFICATION_ID, L3_NAME, L4_NAME, L5_NAME, "" as "L6_NAME", L5_NAME as "Panchayat", CROP_NAME, SUM_INSURED, PREMIUM_RATE_PCT, NOTIFICATION_STATUS from PMFBY.BSB_NOTIFICATION where configid=' + \
            str(configid)

        if district != 'all':
            query += ' and L3_NAME="' + str(district) + '"'

        if 'crop_name' in request.json:
            crop_name = request.json['crop_name']
            if len(crop_name) == 0:
                message = {
                    'status': 502,
                    'message': "Crop List is empty"
                }
                response = jsonify(message)
                response.status_code = 502
                pool.release(conn)
                gc.collect()
                return response

            crop_list = '('
            for x in crop_name:
                crop_list += '"' + str(x) + '",'
            crop_list = crop_list[:-1] + ")"
            print(crop_list)
            query += ' and CROP_NAME in ' + str(crop_list)

        print(query)
        data = cx.read_sql(read_connection_string, query,
                           partition_on="NOTIFICATION_ID", partition_num=10)
        data = data.sort_values('NOTIFICATION_ID')
        print(len(data))
        j = data.to_dict('records')
        # print(j)
        resp = {
            "data": j,
            "col_name": col_name
        }
        response = jsonify(resp)
        response.status_code = 200
        pool.release(conn)
        gc.collect()
        return response
    except Exception as e:
        print("Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        message = {
            'status': 502,
            'message': str(e)
        }
        response = jsonify(message)
        response.status_code = 502
        pool.release(conn)
        gc.collect()
        return response


@pmfby.route('/pmfby_notification_download_filter', methods=['GET'])
def pmfby_notification_download_filter():
    try:

        global pool
        print(pool)
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

        configid = request.args.get('configid')
        district = request.args.get('district')
        search_text = request.args.get('search_text')
        crop_list = ""
        print("------------request.args------------", request.args)
        print('notification_download ', configid,
              search_text, district)
        print("-----search------", type(search_text))
        columns = ['NOTIFICATION_ID', 'PMFBY_NOTIF_ID', 'CROP_NAME', 'CROP_CODE', 'SSSYID', 'STATE', 'STATE_CODE', 'L3_NAME_CODE', 'L3_NAME', 'L4_NAME', 'L4_NAME_CODE', 'L4_TERM',	'L5_NAME', 'L5_NAME_CODE', 'L5_TERM', 'L6_NAME', 'L6_NAME_CODE', 'L6_TERM', 'L7_NAME', 'L7_NAME_CODE', 'INSURANCE_UNIT_NAME', 'INSURANCE_COMPANY_CODE',	'INSURANCE_COMPANY_NAME', 'PREMIUM_RATE_PCT', 'FARMER_SHARE_PCT', 'GOI_SHARE_PCT', 'STATE_SHARE_PCT	INDEMNITY', 'SUM_INSURED', 'CUT_OFF_DATE',
                   'NOTIFIED_IU_NAME', 'GOVT_THRESHOLD_YIELD', 'DRIAGE_FACTOR', 'EXPECTED_SUM_INSURED', 'EXPECTED_PREMIUM', 'NO_OF_CCE_REQD', 'CCE_AREA', 'CONFIGID', 'UPLOAD_TASK_ID', 'UPDATED_USERID', 'UPDATED_DATE', 'NOTIFICATION_STATUS', 'LAST_DENOTIFY_DATE']

        print("------------test else------------")
        query = 'select NOTIFICATION_ID,PMFBY_NOTIF_ID,CROP_NAME,CROP_CODE,SSSYID,STATE,STATE_CODE,L3_NAME_CODE,L3_NAME,L4_NAME,L4_NAME_CODE,L4_TERM,L5_NAME,L5_NAME_CODE,L5_TERM,L6_NAME,L6_NAME_CODE,L6_TERM,L7_NAME,L7_NAME_CODE,INSURANCE_UNIT_NAME,INSURANCE_COMPANY_CODE,INSURANCE_COMPANY_NAME,PREMIUM_RATE_PCT,FARMER_SHARE_PCT,GOI_SHARE_PCT,STATE_SHARE_PCT,INDEMNITY,SUM_INSURED,NOTIFIED_IU_NAME,GOVT_THRESHOLD_YIELD,DRIAGE_FACTOR,EXPECTED_SUM_INSURED,EXPECTED_PREMIUM,NO_OF_CCE_REQD,CCE_AREA,CONFIGID,UPLOAD_TASK_ID,UPDATED_USERID,UPDATED_DATE,NOTIFICATION_STATUS,LAST_DENOTIFY_DATE from PMFBY.NOTIFICATION where configid=' + \
            str(configid)

        if district != 'all':
            query += ' and L3_NAME="' + str(district) + '"'

        if 'crop_name' in request.args:
            crop_name = request.args.get('crop_name')
            if len(crop_name) == 0:
                message = {
                    'status': 502,
                    'message': "Crop List is empty"
                }
                response = jsonify(message)
                response.status_code = 502
                pool.release(conn)
                gc.collect()
                return response
            crop_list = crop_name.replace("[", "(").replace("]", ")")
            print("----crop_list----", crop_list)
            query += ' and CROP_NAME in ' + str(crop_list)

        if search_text:
            print("------------test if------------")
            query += " and (L3_NAME like '%"+str(search_text)+"%' or L4_NAME like '%"+str(search_text)+"%' or L5_NAME like '%"+str(search_text)+"%' or L6_NAME like '%"+str(search_text)+"%' or L7_NAME like '%"+str(
                search_text)+"%' or SUM_INSURED like '%"+str(search_text)+"%' or PREMIUM_RATE_PCT like '%"+str(search_text)+"%' or CROP_NAME like '%"+str(search_text)+"%' or NOTIFICATION_ID like '%"+str(search_text)+"%')"

        print(query)
        b = io.BytesIO()
        data = cx.read_sql(read_connection_string, query,
                           partition_on="NOTIFICATION_ID", partition_num=10)
        data = data.sort_values('NOTIFICATION_ID')
        data.columns = columns
        writer = pd.ExcelWriter(b, engine='xlsxwriter')
        data.to_excel(excel_writer=writer, index=False,
                      sheet_name='NOTIFICATION')

        with conn.cursor() as cur:
            # change product name
            query = 'select state, product, season, year from PMFBY.PRODUCT_CONFIG where configid=' + \
                str(configid)
            print("Query: ", query)
            cur.execute(query)
            result = cur.fetchone()
            print(result)
        state = result['state']
        # change product name
        product = result['product']
        season = result['season']
        year = result['year']

        report_param = {
            'STATE': [state],
            'PRODUCT': [product],
            'SEASON': [season],
            'YEAR': [year],
            'DISTRICT': [district],
            'CROP': [crop_list],
            'SEARCH TEXT': [search_text]
        }

        data = pd.DataFrame(report_param, columns=[
                            'STATE', 'PRODUCT', 'SEASON', 'YEAR', 'DISTRICT', 'CROP', 'SEARCH TEXT'])
        data.to_excel(excel_writer=writer, index=False,
                      sheet_name='SELECTION PARAMETERS')
        # writer.save()
        writer.close()
        response = Response(b.getvalue(), mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            headers={"Content-disposition": "attachment; filename=PMFBY_Notification_"+str(configid)+".xlsx"})
        response.status_code = 200
        pool.release(conn)
        gc.collect()
        return response

    except Exception as e:
        print("Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        message = {
            'status': 502,
            'message': str(e)
        }
        response = jsonify(message)
        response.status_code = 502
        pool.release(conn)
        gc.collect()
        return response


@pmfby.route('/get_pmfby_proposal', methods=['GET'])
def get_proposal():
    try:

        global pool
        print(pool)
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

        print(request.json)
        configid = request.json['configid']
        district = request.json['district']
        status = request.json['status']
        method = request.json['method']

        # query = 'select PROPOSAL_ID, FARMER_APPLICATION_ID, CROP_L3_NAME, CROP_L4_NAME, CROP_L5_NAME, CROP_L6_NAME, CROP_L7_NAME, FARMER_CROP_NAME, FARMER_AREA_INSURED, SUM_INSURED, GROSS_PREMIUM, PROPOSAL_STATUS from PMFBY.PROPOSAL where configid=' + \
        #     str(configid)

        if method in ['PMFBY_Underwriting_Report', 'RWBCIS_Underwriting_Report']:
            query = '''select
                        CROP_L3_NAME as "DISTRICT_NAME",
                        CROP_L3_CODE as "DISTRICT_CODE",
                        count(PROPOSAL_ID) as "TOTAL_POLICIES",
                        round(sum(GROSS_PREMIUM),2) as "GROSS_PREMIUM",
                        round(sum(FARMER_SHAREOF_PREMIUM),2) as "FARMER_SHARE",
                        round(sum(SUM_INSURED),2) as "SUM_INSURED",
                        round(sum(FARMER_AREA_INSURED),2) as "AREA_INSURED"
                        from PMFBY.PROPOSAL
                        where
                        CONFIGID=''' + str(configid)

            if status != '%':
                query += '''
                        and PROPOSAL_STATUS="''' + str(status) + '''"'''

            if district != 'all':
                query += '''
                        and CROP_L3_CODE=(select L3_NAME_CODE from PMFBY.NOTIFICATION where configid=''' + str(configid) + ''' and L3_NAME = "''' + str(district) + '''" limit 1)'''

            if 'crop_name' in request.json:
                crop_name = request.json['crop_name']
                if len(crop_name) == 0:
                    message = {
                        'status': 502,
                        'message': "Crop List is empty"
                    }
                    response = jsonify(message)
                    response.status_code = 502
                    pool.release(conn)
                    gc.collect()
                    return response

                crop_list = '('
                for x in crop_name:
                    crop_list += '"' + str(x) + '",'
                crop_list = crop_list[:-1] + ")"
                print(crop_list)
                query += '''
                        and FARMER_CROP_NAME in ''' + str(crop_list)

            query += '''
                        group by CROP_L3_CODE'''
        
        elif method == 'BSB_Underwriting_Report':
            query = '''select
                        CROP_L3_NAME as "DISTRICT_NAME",
                        "NA" as "DISTRICT_CODE",
                        count(PROPOSAL_ID) as "TOTAL_POLICIES",
                        round(sum(GROSS_PREMIUM),2) as "GROSS_PREMIUM",
                        "NA" as "FARMER_SHARE",
                        round(sum(SUM_INSURED),2) as "SUM_INSURED",
                        round(sum(FARMER_AREA_INSURED),2) as "AREA_INSURED"
                        from PMFBY.BSB_PROPOSAL
                        where
                        CONFIGID=''' + str(configid)

            if status != '%':
                query += '''
                        and PROPOSAL_STATUS="''' + str(status) + '''"'''

            if district != 'all':
                query += '''
                        and CROP_L3_NAME = "''' + str(district) + '''"'''

            if 'crop_name' in request.json:
                crop_name = request.json['crop_name']
                if len(crop_name) == 0:
                    message = {
                        'status': 502,
                        'message': "Crop List is empty"
                    }
                    response = jsonify(message)
                    response.status_code = 502
                    pool.release(conn)
                    gc.collect()
                    return response

                crop_list = '('
                for x in crop_name:
                    crop_list += '"' + str(x) + '",'
                crop_list = crop_list[:-1] + ")"
                print(crop_list)
                query += '''
                        and FARMER_CROP_NAME in ''' + str(crop_list)

            query += '''
                        group by CROP_L3_NAME'''
        print(query)

        data = cx.read_sql(read_connection_string, query, partition_num=1)
        data = data.sort_values('DISTRICT_NAME')
        print(len(data))
        j = data.to_dict('records')
        # print(j)
        response = jsonify(j)
        response.status_code = 200
        pool.release(conn)
        gc.collect()
        return response

    except Exception as e:
        print("Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        message = {
            'status': 502,
            'message': str(e)
        }
        response = jsonify(message)
        response.status_code = 502
        pool.release(conn)
        gc.collect()
        return response


def read_files_from_s3(upload_log_id: int, filetype: str, filepath: str):
    """
    Reads all files from S3 folder named as "upload_log_id" 
    based on "filetype" provided (csv/excel)
    """
    print("Reading file: ", upload_log_id, filetype)
    try:
        def get_file(upload_log_id):
            # print ("!!!!!! ", len(filenames))
            paginator = s3.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket='sdg-01', Prefix=filepath + str(upload_log_id) + '/')
            for page in page_iterator:
                if page['KeyCount'] > 0:
                    print(upload_log_id, page)
                    for file in page['Contents']:
                        if("." in file['Key']):
                            print(upload_log_id, file)
                            yield file['Key']

        def loop(file, type):
            if type == 'excel':
                fs = s3.get_object(Bucket='sdg-01', Key=file)['Body'].read()
                df = pd.read_excel(fs, keep_default_na=False,
                                   dtype=str)
            elif type == 'csv':
                fs = s3.get_object(Bucket='sdg-01', Key=file).get('Body')
                df = pd.read_csv(fs, encoding='utf8', low_memory=False,
                                 keep_default_na=False, dtype=str)
            df['FILENAME'] = file[file.rindex("/")+1:]
            return df

        file_list = Parallel(n_jobs=-1, verbose=10, prefer="threads")(
            [delayed(loop)(file, filetype) for file in get_file(upload_log_id)])
        df = pd.concat(file_list, ignore_index=True)
        return df

    except Exception as e:
        print("Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        gc.collect()
        return {
            "error": str(e)
        }


def upload_thread_DF_init(conn: pymysql.connections.Connection, upload_log_id: int, columns: list, exclude_columns: list = [], optional_columns: dict = {}, header_rename: dict = None):
    """
    Performs the standard initial steps for file upload
    and return the data in a Pandas Dataframe

    :param pymysql.connections.Connection conn: pymysql connection to the concerned database
    :param upload_log_id int: primary key of the log entry
    :param columns list: list of valid headers
    :param exclude_columns list: list of columns to exclude from validation
    :param optional_columns dict: list of columns to be added if missing, along with default value
    :param header_rename dict: dictionary for renaming dataframe headers
    """
    try:
        # ***** Process Start *****
        with conn.cursor() as cur:
            query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE=0, PROCESS_START_TIME=now() where ID=" + \
                str(upload_log_id)
            print(upload_log_id, query)
            cur.execute(query)
            conn.commit()

        # ***** Fetch file type *****
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
            return 0

        # ***** Read files from s3 *****
        excel_data1 = read_files_from_s3(
            upload_log_id, filetype, s3_user_file_path)
        if type(excel_data1) is dict:
            with conn.cursor() as cur:
                e_str = str(excel_data1["error"]).replace('"', '\\"')
                e_str = e_str.replace("'", "\\'")
                query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Error while reading file', EXCEPTION = '" + e_str + "' where id = " + \
                    str(upload_log_id)+";"
                print("Query: ", query)
                cur.execute(query)
                conn.commit()
            return 0

        # ***** Check for empty file ******
        elif excel_data1.empty:
            with conn.cursor() as cur:
                query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Upload file(s) is/are empty' where id = " + \
                    str(upload_log_id)+";"
                print("Query: ", query)
                cur.execute(query)
                conn.commit()
            return 0

        print(upload_log_id, excel_data1.shape[0])

        # ***** Check if headers are correct *****
        excel_data1.columns = map(str.upper, excel_data1.columns)

        for key, value in optional_columns.items():
            if key not in excel_data1.columns:
                excel_data1[key] = value

        headers = list(excel_data1.columns.values)

        for elem in exclude_columns:
            headers.remove(elem)

        print(headers)

        # if(not set(headers).issubset(set(columns))):
        if(headers != columns):
            with conn.cursor() as cur:
                query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Headers do not match' where id = " + \
                    str(upload_log_id)+";"
                print("Query: ", query)
                cur.execute(query)
                conn.commit()
            return 0

        if header_rename is not None:
            excel_data1.rename(columns=header_rename, inplace=True)
        excel_data1 = excel_data1.replace('\\\\', '\\\\\\\\', regex=True)
        return excel_data1

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
        return 0


def upload_file_to_DB(conn: pymysql.connections.Connection, data_csv: pd.DataFrame, load_file_path: str, table_name: str):
    try:
        data_csv.to_csv(load_file_path, encoding='utf-8', header=True,
                        doublequote=True, sep=',', index=False, na_rep='\\N', quotechar='"')

        with conn.cursor() as cur:
            load_sql = "LOAD DATA LOCAL INFILE '" + load_file_path + \
                "' INTO TABLE " + \
                str(table_name) + \
                " FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES;"
            print(load_sql)
            cur.execute(load_sql)
            conn.commit()

        os.remove(load_file_path)

        return 1

    except Exception as e:
        return e


def notifiction_upload_thread(user_id: int, configid: int, upload_log_id: int, season: str, year: int, state: str, state_code: int):
    """
    Reads Notification file from AWS S3 based on "upload_log_id" and insert the same in Notification table
    """
    global notif_row, pool
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

        dictionary = {'CROP NOTIFICATIONS ID*': 'PMFBY_NOTIF_ID', 'CROP NAME*': 'CROP_NAME',
                      'CROP CODE*': 'CROP_CODE', 'SSSY ID*': 'SSSYID', 'STATE NAME*': 'STATE',
                      'STATE CODE*': 'STATE_CODE', 'DISTRICT CODE*': 'L3_NAME_CODE',
                      'DISTRICT NAME*': 'L3_NAME', 'LEVEL 4 NAME*': 'L4_NAME',
                      'LEVEL 4 CODE*': 'L4_NAME_CODE', 'LEVEL 4*': 'L4_TERM',
                      'LEVEL 5 NAME*': 'L5_NAME', 'LEVEL 5 CODE*': 'L5_NAME_CODE',
                      'LEVEL 5*': 'L5_TERM', 'LEVEL 6 NAME*': 'L6_NAME',
                      'LEVEL 6 CODE*': 'L6_NAME_CODE', 'LEVEL 6*': 'L6_TERM',
                      'VILLAGE NAME*': 'L7_NAME', 'VILLAGE CODE*': 'L7_NAME_CODE',
                      'INSURANCE UNIT NAME*': 'INSURANCE_UNIT_NAME', 'INSURANCE COMPANY CODE*': 'INSURANCE_COMPANY_CODE',
                      'INSURANCE COMPANY NAME*': 'INSURANCE_COMPANY_NAME', 'PREMIUM RATE*': 'PREMIUM_RATE_PCT',
                      'FARMER SHARE*': 'FARMER_SHARE_PCT', 'GOI SHARE*': 'GOI_SHARE_PCT',
                      'STATE SHARE*': 'STATE_SHARE_PCT', 'INDEMNITY LEVEL*': 'INDEMNITY',
                      'SUM INSURED*': 'SUM_INSURED', 'CUT OF DATE*': 'CUT_OFF_DATE',
                      'NOTIFIED IU NAME*': 'NOTIFIED_IU_NAME', 'GOVT THRESHOLD YIELD (KG/HECTARE)*': 'GOVT_THRESHOLD_YIELD',
                      'EXPECTED DRIAGE FACTOR (%)*': 'DRIAGE_FACTOR', 'EXPECTED SUM INSURED (RS.)*': 'EXPECTED_SUM_INSURED',
                      'NO. OF CCE REQD.*': 'NO_OF_CCE_REQD', 'CCE AREA (SQ. M)*': 'CCE_AREA'}

        excel_data1 = upload_thread_DF_init(conn, upload_log_id, notif_row, exclude_columns=[
                                            'FILENAME'], header_rename=dictionary)
        if type(excel_data1) != pd.DataFrame:
            pool.release(conn)
            gc.collect()
            return

        time_now = datetime.now()
        time_get = time_now.strftime("%Y%m%d%H%M%S")
        updated_date_time = time_now.strftime("%Y-%m-%d %H:%M:%S")
        file_name = 'noti_'+str(user_id)+"_" + \
            str(configid)+"_"+str(time_get)+".csv"
        # table_name = 'AIC_STAGING.'+str(file_name[:-5])
        # print(table_name)

        no_of_records_uploaded = excel_data1.shape[0]

        exist_data = cx.read_sql(read_connection_string, "SELECT NOTIFICATION_ID, CROP_CODE, CONFIGID, L3_NAME_CODE, L4_NAME_CODE, L5_NAME_CODE, L6_NAME_CODE, L7_NAME_CODE FROM PMFBY.NOTIFICATION WHERE CONFIGID=" +
                                 str(configid), partition_on="NOTIFICATION_ID", partition_num=10)

        exist_data = exist_data.apply(
            lambda x: x.astype(str).str.upper().str.strip())

        # excel_data1['EXPECTED_PREMIUM'] = None
        # excel_data1['NO_OF_CCE_REQD'] = None
        # excel_data1['ACF'] = 1

        excel_data1['GOVT_ACTUAL_YIELD'] = None
        excel_data1['PREVENTED_SOWING_LOSS_PCT'] = None
        excel_data1['MID_SEASON_ESTIMATED_YIELD'] = None
        excel_data1['PARAMETRIC_EOS_LOSS_PCT'] = None

        excel_data1['CONFIGID'] = configid

        excel_data1['UPLOAD_TASK_ID'] = upload_log_id

        excel_data1['UPDATED_USERID'] = user_id

        excel_data1['UPDATED_DATE'] = updated_date_time

        excel_data1['NOTIFICATION_STATUS'] = 1

        excel_data1['LAST_DENOTIFY_DATE'] = None

        excel_data1 = excel_data1.apply(
            lambda x: x.astype(str).str.upper().str.strip())

        excel_data1 = excel_data1.replace('\\\\', '\\\\\\\\', regex=True)

        excel_data1[['CROP_CODE', 'STATE_CODE', 'L3_NAME_CODE', 'L4_NAME_CODE', 'L5_NAME_CODE', 'L6_NAME_CODE', 'L7_NAME_CODE']] = excel_data1[['CROP_CODE', 'STATE_CODE', 'L3_NAME_CODE', 'L4_NAME_CODE', 'L5_NAME_CODE', 'L6_NAME_CODE', 'L7_NAME_CODE']].apply(
            lambda x: x.str.lstrip('0'))

        excel_data1 = excel_data1.replace('', None)

        excel_data1['CALC_PREMIUM'] = pd.to_numeric(excel_data1['FARMER_SHARE_PCT'], errors='coerce') + pd.to_numeric(
            excel_data1['STATE_SHARE_PCT'], errors='coerce') + pd.to_numeric(excel_data1['GOI_SHARE_PCT'], errors='coerce')

        duplicate_list = ['CROP_CODE', 'CONFIGID']
        de_duplicate_list = ['PREMIUM_RATE_PCT', 'FARMER_SHARE_PCT', 'GOI_SHARE_PCT', 'STATE_SHARE_PCT',
                             'INDEMNITY', 'SUM_INSURED', 'CUT_OFF_DATE', 'NOTIFIED_IU_NAME', 'INSURANCE_UNIT_NAME']

        with conn.cursor() as cur:
            query = "select * from PMFBY.LOCATION_HIERARCHY where STATE_CODE=" + \
                str(state_code)+";"
            print("Query: ", query)
            cur.execute(query)
            hierarchy_data = cur.fetchone()

        print("Hierarchy Data: ", hierarchy_data)

        valuesToBeChecked = {
            'PMFBY_NOTIF_ID': None,
            'CROP_NAME': None,
            'CROP_CODE': None,
            'SSSYID': None,
            'STATE': [state],
            'STATE_CODE': [state_code],
            'INSURANCE_UNIT_NAME': None,
            'PREMIUM_RATE_PCT': None,
            'FARMER_SHARE_PCT': None,
            'GOI_SHARE_PCT': None,
            'STATE_SHARE_PCT': None,
            'INDEMNITY': None,
            'SUM_INSURED': None,
            'NOTIFIED_IU_NAME': None
        }

        numericColumns = {
            'PREMIUM_RATE_PCT': 'isnumeric',
            'FARMER_SHARE_PCT': 'isnumeric',
            'GOI_SHARE_PCT': 'isnumeric',
            'STATE_SHARE_PCT': 'isnumeric',
            'INDEMNITY': 'isnumeric',
            'SUM_INSURED': 'isnumeric',
            'GOVT_THRESHOLD_YIELD': 'isnumeric',
            'DRIAGE_FACTOR': 'isnumeric',
            'EXPECTED_SUM_INSURED': 'isnumeric',
            'NO_OF_CCE_REQD': 'isinteger',
            'CCE_AREA': 'isnumeric',
            'L3_NAME_CODE': 'isinteger',
            'L4_NAME_CODE': 'isinteger',
            'L5_NAME_CODE': 'isinteger',
            'L6_NAME_CODE': 'isinteger',
            'L7_NAME_CODE': 'isinteger',
        }

        premium_check = {
            'PREMIUM_RATE_PCT': {'CALC_PREMIUM': [0, 2]}
        }

        if hierarchy_data['L3'] is not None:
            valuesToBeChecked['L3_NAME_CODE'] = None
            valuesToBeChecked['L3_NAME'] = None
            duplicate_list.append('L3_NAME_CODE')

        if hierarchy_data['L4'] is not None:
            valuesToBeChecked['L4_NAME'] = None
            valuesToBeChecked['L4_NAME_CODE'] = None
            valuesToBeChecked['L4_TERM'] = None
            duplicate_list.append('L4_NAME_CODE')

        if hierarchy_data['L5'] is not None:
            valuesToBeChecked['L5_NAME'] = None
            valuesToBeChecked['L5_NAME_CODE'] = None
            valuesToBeChecked['L5_TERM'] = None
            duplicate_list.append('L5_NAME_CODE')

        if hierarchy_data['L6'] is not None:
            valuesToBeChecked['L6_NAME'] = None
            valuesToBeChecked['L6_NAME_CODE'] = None
            valuesToBeChecked['L6_TERM'] = None
            duplicate_list.append('L6_NAME_CODE')

        if hierarchy_data['L7'] is not None:
            valuesToBeChecked['L7_NAME'] = None
            valuesToBeChecked['L7_NAME_CODE'] = None
            duplicate_list.append('L7_NAME_CODE')

        dfObject = CheckValidity(excel_data1)
        exec_status = dfObject.removeDuplicateFromDf(
            exist_data, duplicate_list, "DUPLICATE NOTIFICATION EXISTS IN DATABASE")
        if exec_status != True:
            raise Exception(exec_status)

        exec_status = dfObject.removeDuplicateSelf(
            duplicate_list, "DUPLICATE NOTIFICATION IN FILE", True, de_duplicate_list)
        if exec_status != True:
            raise Exception(exec_status)

        exec_status = dfObject.checkValue(valuesToBeChecked)
        if exec_status != True:
            raise Exception(exec_status)

        exec_status = dfObject.checkValue(numericColumns)
        if exec_status != True:
            raise Exception(exec_status)
        
        exec_status = dfObject.checkValue(premium_check)
        if exec_status != True:
            raise Exception(exec_status)

        data_csv = dfObject.getValidDf().drop(
            ['REMARK', 'BUSINESS_LOGIC_VALID', 'CALC_PREMIUM'], errors='ignore', axis=1)

        # data_csv['EXPECTED_PREMIUM'] = None

        data_csv.insert(0, "NOTIFICATION_ID", None)

        # Insert feeback in to S3
        buf = io.BytesIO()
        feedback_df = dfObject.getInvalidDf().drop(['NO_OF_CCE_REQD', 'CONFIGID', 'UPLOAD_TASK_ID', 'UPDATED_USERID',
                                                    'UPDATED_DATE', 'NOTIFICATION_STATUS', 'LAST_DENOTIFY_DATE', 'BUSINESS_LOGIC_VALID'], errors='ignore', axis=1)
        feedback_df.to_csv(buf, encoding='utf-8', index=False)

        buf.seek(0)
        s3.put_object(Body=buf, Bucket='sdg-01',
                      Key=s3_feedback_path + str(upload_log_id) + "/Feedback.csv")

        load_file_path = file_path + "/" + file_name

        # ***** upload csv file to DB *****
        up_status = upload_file_to_DB(
            conn, data_csv, load_file_path, "PMFBY.NOTIFICATION")
        if up_status != 1:
            raise Exception(up_status)

        # data_csv['EXPECTED_PREMIUM'] = None

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


def uw_upload_thread(user_id: int, configid: int, upload_log_id: int, season: str, product: str, year: int, state: str, state_code: int):
    # print("before sleep")
    # time.sleep(20)
    # print("after sleep")
    """
    Reads Proposal file from AWS S3 based on "upload_log_id" and insert the same in Proposal table

    """
    try:
        global uw_row, pool
        print("1", upload_log_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        time_now = datetime.now()

        year = str(year)
        print(user_id, configid, upload_log_id,
              season, product, year, state, state_code)
        mmyy = time_now.strftime("%m") + time_now.strftime("%Y")[-2:]
        season_code = season[:2] + str(year)[-2:] + str(int(year)+1)[-2:]
        account_tag = "/".join([str(i)
                                for i in [product, state_code, season_code, mmyy]])
        # print(pool)
        # *****creating connection*****
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
        # print(conn)

        dictionary = {"SCHEME NAME": "PRODUCT_NAME",	"YEAR": "YEAR",	"SEASON": "SEASON",	"STATE NAME": "STATE",	"APPLICATION ID": "FARMER_APPLICATION_ID",	"FARMER ID": "FARMER_ID",	"FARMER NAME": "FARMER_NAME",	"FARMER ID TYPE": "FARMER_ID_TYPE",	"FARMER ID NUMBER": "FARMER_ID_NO",	"RELATIVE NAME": "FARMER_RELATIVE_NAME",	"RELATIVE'S RELATION": "FARMER_RELATIVE_RELATION",	"FARMER MOBILE": "FARMER_MOBILE",	"AGE": "FARMER_AGE",	"GENDER": "FARMER_GENDER",	"COMMUNITY": "FARMER_CASTE",	"ADDRESS": "FARMER_ADDRESS",	"STATE OF RESIDENCE": "FARMER_L2_NAME",	"STATE CODE (RESIDENCE)": "FARMER_L2_CODE",	"DISTRICT OF RESIDENCE": "FARMER_L3_NAME",	"DISTRICT CODE (RESIDENCE)": "FARMER_L3_CODE",	"SUB DISTRICT NAME OF RESIDENCE": "FARMER_L4_NAME",	"SUB DISTRICT CODE (RESIDENCE)": "FARMER_L4_CODE",	"VILLAGE OF RESIDENCE": "FARMER_L7_NAME",	"VILLAGE CODE(RESIDENCE)": "FARMER_L7_CODE",	"PINCODE": "FARMER_PINCODE",	"BANK NAME": "BANK_NAME",	"BANK ID": "BANK_ID",	"BRANCH NAME": "BANK_BRANCH_NAME",	"BRANCH ID": "BANK_BRANCH_ID",	"IFSC": "IFSC",	"FARMER TYPE": "FARMER_TYPE",	"ACCOUNT OWNERSHIP TYPE": "ACCOUNT_OWNERSHIP_TYPE",	"ACCOUNT NUMBER": "FARMER_BANK_ACCOUNT_NO",	"FARMER CATEGORY": "FARMER_CATEGORY",	"NATURE OF FARMER": "FARMER_NATURE",	"FARMER ACCOUNT TYPE": "FARMER_ACCOUNT_TYPE",	"INSURANCE COMPANY NAME": "INSURANCE_COMPANY_NAME",	"INUSRANCE COMPANY CODE": "INSURANCE_COMPANY_CODE",	"CROP NAME": "FARMER_CROP_NAME",	"CROP CODE": "CROP_CODE",	"CROP STATE NAME": "CROP_L2_NAME",	"CROP STATE CODE": "CROP_L2_CODE",	"CROP DISTRICT NAME": "CROP_L3_NAME",	"CROP DISTRICT CODE": "CROP_L3_CODE",	"IU LEVEL": "INSURANCE_UNIT_LEVEL",
                      "IU NAME": "INSURANCE_UNIT_NAME",	"IU CODE": "INSURANCE_UNIT_CODE",	"CROP VILLAGE NAME": "CROP_L7_NAME",	"CROP VILLAGE CODE": "CROP_L7_CODE",	"LAND SURVEY NUMBER": "LAND_SURVEY_NUMBER",	"LAND SUBDIVISION NUMBER": "LAND_SUB_DIVISION_NUMBER",	"AREA INSURED": "FARMER_AREA_INSURED",	"FARMER SHARE": "FARMER_SHAREOF_PREMIUM",	"STATE SHARE": "STATE_SHAREOF_PREMIUM",	"GOI SHARE": "GOI_SHAREOF_PREMIUM",	"GROSS PREMIUM": "GROSS_PREMIUM",	"SUM INSURED": "SUM_INSURED",	"MIX CROP": "MIX_CROP",	"COMBINED AREA INSURED": "COMBINED_INSURED_AREA",	"PREMIUM DEBIT DATE": "PREMIUM_DEBIT_DATE",	"SOWING DATE": "SOWING_DATE",	"CUT OF DATE": "CUT_OFF_DATE",	"INDEMNITY LEVEL": "INDEMNITY",	"APPLICATION STATUS": "APPLICATION_STATUS",	"APPLICATION SOURCE": "APPLICATION_SOURCE",	"UTR/TRANSACTION NUMBER": "UTR_TRANSACTION_NUMBER",	"UTR/TRANSACTION AMOUNT": "UTR_TRANSACTION_AMOUNT",	"UTR/TRANSACTION DATE": "UTR_TRANSACTION_DATE",	"UTR/TRANSACTION MODE": "UTR_TRANSACTION_MODE",	"LEVEL4NAME": "CROP_L4_NAME",	"LEVEL4CODE": "CROP_L4_CODE",	"LEVEL5NAME": "CROP_L5_NAME",	"LEVEL5CODE": "CROP_L5_CODE",	"LEVEL6NAME": "CROP_L6_NAME",	"LEVEL6CODE": "CROP_L6_CODE",	"TIMESTAMP DATA GENERATION": "TIMESTAMP_DATA_GENERATION",	"CREATED BANK NAME": "CREATED_BANK_NAME",	"CREATED BANK CODE": "CREATED_BANK_ID",	"CREATED BRANCH NAME": "CREATED_BANK_BRANCH_NAME",	"CREATED BRANCH CODE": "CREATED_BANK_BRANCH_ID",	"CREATED USER NAME": "CREATED_USER_NAME",	"CSC ID": "CSC_ID",	"APPLICATION CREATED DATE": "APPLICATION_CREATED_DATE",	"LAND RECORD FARMER NAME": "LAND_RECORD_FARMER_NAME",	"LAND RECORD AREA": "LAND_RECORD_AREA"}

        duplicate_list = ['YEAR', 'SEASON', 'STATE', 'FARMER_NAME',
                          'CROP_CODE', 'LAND_SURVEY_NUMBER', 'LAND_SUB_DIVISION_NUMBER']
        de_duplicate_list = ['FARMER_ID', 'IFSC']
        notif_match_list = ['CROP_CODE']
        valuesToBeChecked = {
            'STATE': [state],
            'YEAR': [year],
            'SEASON': [season],
            'FARMER_APPLICATION_ID': None,
            'FARMER_ID': None,
            'FARMER_NAME': None,
            'FARMER_ID_TYPE': None,
            'FARMER_ID_NO': None,
            'FARMER_RELATIVE_NAME': None,
            'FARMER_MOBILE': None,
            'BANK_NAME': None,
            'BANK_ID': None,
            'BANK_BRANCH_NAME': None,
            'BANK_BRANCH_ID': None,
            'IFSC': None,
            'FARMER_TYPE': None,
            'FARMER_BANK_ACCOUNT_NO': None,
            'FARMER_CATEGORY': None,
            'CROP_CODE': None,
            'FARMER_CROP_NAME': None,
            'CROP_L2_NAME': [state],
            'CROP_L2_CODE': [state_code],
            'INSURANCE_UNIT_LEVEL': None,
            'INSURANCE_UNIT_CODE': None,
            'LAND_SURVEY_NUMBER': None,
            'FARMER_AREA_INSURED': None,
            'SUM_INSURED': None,
            'GROSS_PREMIUM': None,
            'FARMER_SHAREOF_PREMIUM': None,
            'STATE_SHAREOF_PREMIUM': None,
            'GOI_SHAREOF_PREMIUM': None,
            'APPLICATION_STATUS': ['APPROVED', 'PENDING FOR APPROVAL(DECLARATION)', 'REJECTED', 'CSC REVERTED'],
            'APPLICATION_SOURCE': None
        }

        numericColumns = {
            'FARMER_AREA_INSURED': 'isnumeric',
            'SUM_INSURED': 'isnumeric',
            'GROSS_PREMIUM': 'isnumeric',
            'FARMER_SHAREOF_PREMIUM': 'isnumeric',
            'STATE_SHAREOF_PREMIUM': 'isnumeric',
            'GOI_SHAREOF_PREMIUM': 'isnumeric',
        }

        calc_fields = '''
            CALC_SUM_INSURED = NOTIF_SI * FARMER_AREA_INSURED
            CALC_FARMER_SHAREOF_PREMIUM = NOTIF_SI * FARMER_AREA_INSURED * FARMER_SHARE_PCT / 100
            CALC_STATE_SHAREOF_PREMIUM = NOTIF_SI * FARMER_AREA_INSURED * STATE_SHARE_PCT / 100
            CALC_GOI_SHAREOF_PREMIUM = NOTIF_SI * FARMER_AREA_INSURED * GOI_SHARE_PCT / 100
            CALC_GROSS_PREMIUM = NOTIF_SI * FARMER_AREA_INSURED * PREMIUM_RATE_PCT / 100
        '''

        fin_check = {
            'SUM_INSURED': {'CALC_SUM_INSURED': [1, 2]},
            'FARMER_SHAREOF_PREMIUM': {'CALC_FARMER_SHAREOF_PREMIUM': [1, 2]},
            'STATE_SHAREOF_PREMIUM': {'CALC_STATE_SHAREOF_PREMIUM': [1, 2]},
            'GOI_SHAREOF_PREMIUM': {'CALC_GOI_SHAREOF_PREMIUM': [1, 2]},
            'GROSS_PREMIUM': {'CALC_GROSS_PREMIUM': [1, 2]},
            'CALC_GROSS_PREMIUM_INDIR': {'GROSS_PREMIUM': [1, 2]}
        }

        time_get = time_now.strftime("%Y%m%d%H%M%S")
        updated_date_time = time_now.strftime("%Y-%m-%d %H:%M:%S")
        file_name = 'pro_'+str(user_id)+"_"+str(time_get)

        # *****Get Data in form of DF*****
        csv_file_data1 = upload_thread_DF_init(conn, upload_log_id, uw_row, exclude_columns=[
                                               'FILENAME'], optional_columns={'DUPLICATE': ''}, header_rename=dictionary)
        if type(csv_file_data1) != pd.DataFrame:
            pool.release(conn)
            gc.collect()
            return

        csv_file_data1 = csv_file_data1.apply(
            lambda x: x.astype(str).str.upper().str.strip())

        csv_file_data1 = csv_file_data1.replace('\\\\', '\\\\\\\\', regex=True)

        csv_file_data1[['CROP_CODE', 'CROP_L2_CODE', 'CROP_L3_CODE', 'CROP_L4_CODE', 'CROP_L5_CODE', 'CROP_L6_CODE', 'CROP_L7_CODE']] = csv_file_data1[['CROP_CODE', 'CROP_L2_CODE', 'CROP_L3_CODE', 'CROP_L4_CODE', 'CROP_L5_CODE', 'CROP_L6_CODE', 'CROP_L7_CODE']].apply(
            lambda x: x.str.lstrip('0'))

        csv_file_data1 = csv_file_data1.replace('', None)

        total_records = csv_file_data1.shape[0]

        feedback_file = []

        column_name_excel_template = csv_file_data1.columns.tolist()
        # print(upload_log_id, column_name_excel_template)

        # *****Calculate GROSS PREMIUM*****
        csv_file_data1['CALC_GROSS_PREMIUM_INDIR'] = pd.to_numeric(csv_file_data1['FARMER_SHAREOF_PREMIUM'], errors='coerce') + pd.to_numeric(
            csv_file_data1['STATE_SHAREOF_PREMIUM'], errors='coerce') + pd.to_numeric(csv_file_data1['GOI_SHAREOF_PREMIUM'], errors='coerce')

        # *****Fetching existing proposal data*****
        start_timed = datetime.now()
        exist_data = cx.read_sql(read_connection_string, "SELECT PROPOSAL_ID, YEAR, SEASON, STATE, FARMER_APPLICATION_ID, FARMER_NAME, FARMER_RELATIVE_NAME, CROP_CODE, LAND_SURVEY_NUMBER, LAND_SUB_DIVISION_NUMBER FROM PMFBY.PROPOSAL WHERE PROPOSAL_STATUS<>'REJECTED' AND CONFIGID='" +
                                 str(configid) + "' ", partition_on="PROPOSAL_ID", partition_num=10)
        exist_data = exist_data.apply(
            lambda x: x.astype(str).str.upper().str.strip())
        end_timed = datetime.now()
        # print(upload_log_id, "EXIST DATA covert in df --------------", end_timed-start_timed)

        # *****Transform exist data to drop possible duplicates*****
        exist_data_non_duplicate = exist_data.drop_duplicates(
            subset=duplicate_list, keep='first')
        exist_data_non_duplicate.rename(
            columns={"FARMER_APPLICATION_ID": "FID"}, inplace=True)

        # *****Fetching Notification data*****
        get_notif_query = 'SELECT NOTIFICATION_ID,CROP_CODE,L3_NAME_CODE as "CROP_L3_CODE",L4_NAME_CODE as "CROP_L4_CODE",L5_NAME_CODE as "CROP_L5_CODE",L6_NAME_CODE as "CROP_L6_CODE",L7_NAME_CODE as "CROP_L7_CODE", FARMER_SHARE_PCT, GOI_SHARE_PCT, STATE_SHARE_PCT, PREMIUM_RATE_PCT, SUM_INSURED as "NOTIF_SI" FROM PMFBY.NOTIFICATION where CONFIGID=' + \
            str(configid)

        # notif_data = cx.read_sql(
        #     read_connection_string, get_notif_query, partition_on="NOTIFICATION_ID", partition_num=10)

        notif_data = pd.read_sql(get_notif_query,
                                 read_connection_string, dtype={'CROP_CODE': str, 'CROP_L3_CODE': str, 'CROP_L4_CODE': str, 'CROP_L5_CODE': str, 'CROP_L6_CODE': str, 'CROP_L7_CODE': str})

        notif_data = notif_data.apply(
            lambda x: x.astype(str).str.upper().str.strip())

        notif_data[['FARMER_SHARE_PCT', 'GOI_SHARE_PCT', 'STATE_SHARE_PCT', 'PREMIUM_RATE_PCT', 'NOTIF_SI']] = notif_data[[
            'FARMER_SHARE_PCT', 'GOI_SHARE_PCT', 'STATE_SHARE_PCT', 'PREMIUM_RATE_PCT', 'NOTIF_SI']].apply(pd.to_numeric, errors='coerce')

        csv_file_data1['PROPOSAL_STATUS'] = ""
        csv_file_data1['REMARKS'] = ""
        csv_file_data1['UPLOAD_TASK_ID'] = upload_log_id

        csv_file_data1['UPDATED_DATE'] = updated_date_time
        csv_file_data1['UPDATE_BY'] = user_id

        # *****Fetching hierarchy data*****
        with conn.cursor() as cur:
            query = "select * from PMFBY.LOCATION_HIERARCHY where STATE_CODE=" + \
                str(state_code)+";"
            print("Query: ", query)
            cur.execute(query)
            hierarchy_data = cur.fetchone()

        print("Hierarchy Data: ", hierarchy_data)

        # *****Validity check based on hierarchy data*****
        if hierarchy_data['L3'] is not None:
            valuesToBeChecked['CROP_L3_NAME'] = None
            valuesToBeChecked['CROP_L3_CODE'] = None
            numericColumns['CROP_L3_CODE'] = 'isinteger'
            notif_match_list.append('CROP_L3_CODE')

        if hierarchy_data['L4'] is not None:
            valuesToBeChecked['CROP_L4_NAME'] = None
            valuesToBeChecked['CROP_L4_CODE'] = None
            numericColumns['CROP_L4_CODE'] = 'isinteger'
            notif_match_list.append('CROP_L4_CODE')

        if hierarchy_data['L5'] is not None:
            valuesToBeChecked['CROP_L5_NAME'] = None
            valuesToBeChecked['CROP_L5_CODE'] = None
            numericColumns['CROP_L5_CODE'] = 'isinteger'
            notif_match_list.append('CROP_L5_CODE')

        if hierarchy_data['L6'] is not None:
            valuesToBeChecked['CROP_L6_NAME'] = None
            valuesToBeChecked['CROP_L6_CODE'] = None
            numericColumns['CROP_L6_CODE'] = 'isinteger'
            notif_match_list.append('CROP_L6_CODE')

        if hierarchy_data['L7'] is not None:
            valuesToBeChecked['CROP_L7_NAME'] = None
            valuesToBeChecked['CROP_L7_CODE'] = None
            numericColumns['CROP_L7_CODE'] = 'isinteger'
            notif_match_list.append('CROP_L7_CODE')

        notif_data = notif_data[['NOTIFICATION_ID', 'FARMER_SHARE_PCT', 'GOI_SHARE_PCT',
                                 'STATE_SHARE_PCT', 'PREMIUM_RATE_PCT', 'NOTIF_SI'] + notif_match_list]

        dfObject = CheckValidity(csv_file_data1)

        # *****Removing duplicate APPLICATION_ID from existing data*****
        print("removing duplicate APPLICATION_ID form exist_data")
        exec_status = dfObject.removeDuplicateFromDf(exist_data, [
                                                     'FARMER_APPLICATION_ID'], 'FARMER_APPLICATION_ID ALREADY EXISTS IN DATABASE')
        if exec_status != True:
            raise Exception(exec_status)

        exec_status = dfObject.removeDuplicateSelf(
            ['FARMER_APPLICATION_ID'], 'DUPLICATE FARMER_APPLICATION_ID IN FILE')
        if exec_status != True:
            raise Exception(exec_status)

        # *****Removing duplicate insurances from existing data*****
        print("removing duplicate list from exist_data")
        exec_status = dfObject.removeDuplicateFromDf(
            exist_data_non_duplicate, duplicate_list, 'INSURANCE ALREADY EXISTS IN DATABASE WITH APPLICATION_ID: ', identify_duplicate='FID', allow_duplicate=True)
        if exec_status != True:
            raise Exception(exec_status)

        exec_status = dfObject.removeDuplicateSelf(
            duplicate_list, 'DUPLICATE INSURANCE IN FILE WITH APPLICATION_ID: ', identify_duplicate='FARMER_APPLICATION_ID', allow_duplicate=True)
        if exec_status != True:
            raise Exception(exec_status)

        # *****Check if Notification is present*****
        print("notification check")
        exec_status = dfObject.matchFromDf(
            notif_data, notif_match_list, "NOTIFICATION NOT FOUND")
        if exec_status != True:
            raise Exception(exec_status)

        # *****Data validation for necessary columns and datatypes*****
        exec_status = dfObject.checkValue(numericColumns)
        if exec_status != True:
            raise Exception(exec_status)

        exec_status = dfObject.checkValue(valuesToBeChecked)
        if exec_status != True:
            raise Exception(exec_status)

        # *****Add Calculated Fields*****
        exec_status = dfObject.addColumnFromFormula(calc_fields, "valid")
        if exec_status != True:
            raise Exception(exec_status)
        
        if state != 'ASSAM':
            exec_status = dfObject.checkValue(fin_check)
            if exec_status != True:
                raise Exception(exec_status)

        # *****Add accounting tag*****

        exec_status = dfObject.updateValueFromText(
            {"STATE": [state]}, "equal", {"ACCOUNT_TAG": account_tag})
        if exec_status != True:
            raise Exception(exec_status)

        # *****Add Party ID*****
        exec_status = dfObject.updateValueFromColumn({"APPLICATION_SOURCE": ["BANK", "CBS"]}, "equal", {
                                                     "PARTY_ID": ["CREATED_BANK_ID", "CREATED_BANK_BRANCH_ID", "SEASON", "YEAR"]}, "/")
        if exec_status != True:
            raise Exception(exec_status)

        exec_status = dfObject.updateValueFromColumn({"APPLICATION_SOURCE": ["BANK", "CBS"]}, "not equal", {
                                                     "PARTY_ID": ["FARMER_APPLICATION_ID", "SEASON", "YEAR"]}, "/")
        if exec_status != True:
            raise Exception(exec_status)

        # *****Update PROPOSAL_STATUS*****
        exec_status = dfObject.updateValueFromText({"APPLICATION_STATUS": [
                                                   'PENDING FOR APPROVAL(DECLARATION)', 'REJECTED', 'CSC REVERTED']}, 'equal', {"PROPOSAL_STATUS": "PENDING FOR APPROVAL"})
        if exec_status != True:
            raise Exception(exec_status)
        exec_status = dfObject.updateValueFromText(
            {"APPLICATION_STATUS": ['APPROVED']}, 'equal', {"PROPOSAL_STATUS": "APPROVED"})
        if exec_status != True:
            raise Exception(exec_status)

        # *****Valid Data*****
        data_csv = dfObject.getValidDf()

        # feedback_df.to_excel(file_path + "/" + "feedback.xlsx")
        # data_csv.to_excel(file_path + "/" + "data.xlsx")

        print("valid data: ", data_csv.shape[0])
        no_of_records_success = data_csv.shape[0]
        no_of_records_failed = (total_records-no_of_records_success)
        if no_of_records_success != 0:

            # date = time_now.strftime("%m")
            # acc_tag_year = time_now.strftime("%Y")
            # if state == 'UTTAR PRADESH':
            # 	state_code = "UTP"
            # else:
            # 	state_code = str(state)[0:3]

            # data_csv['ACCOUNT_TAG'] = str(product+'/'+season+'/'+acc_tag_year+'/'+state_code+'/'+date).replace(' ', '')
            data_csv['CONFIGID'] = configid
            pd.to_numeric(data_csv["FARMER_AREA_INSURED"])
            # pd.to_numeric(data_csv["ACF"])

            # data_csv[ 'AREA_AFTER_ACF'] = data_csv[ 'FARMER_AREA_INSURED']
            data_csv = data_csv.drop(
                ['CALC_GROSS_PREMIUM_INDIR', 'FILENAME', 'BUSINESS_LOGIC_VALID', 'REMARK', 'FARMER_SHARE_PCT', 'GOI_SHARE_PCT', 'STATE_SHARE_PCT', 'PREMIUM_RATE_PCT', 'NOTIF_SI', 'DUPLICATE'], errors='ignore', axis=1)
            data_csv.insert(0, "PROPOSAL_ID", None)

        # *****Invalid Data and Feedback*****
        feedback_df = dfObject.getInvalidDf()
        feedback_df['FARMER_APPLICATION_ID'] = "'" + \
            feedback_df['FARMER_APPLICATION_ID']
        feedback_df = feedback_df.drop(
            ['CALC_GROSS_PREMIUM', 'PROPOSAL_STATUS', 'REMARKS', 'UPLOAD_TASK_ID', 'UPDATED_DATE', 'UPDATE_BY', 'DUPLICATE', 'BUSINESS_LOGIC_VALID', 'NOTIFICATION_ID', 'FARMER_SHARE_PCT', 'GOI_SHARE_PCT', 'STATE_SHARE_PCT', 'PREMIUM_RATE_PCT', 'NOTIF_SI', 'FID'], errors='ignore', axis=1
        )

        # *****Upload csv file to db*****
        load_file_path = file_path + "/" + file_name + ".csv"
        up_status = upload_file_to_DB(
            conn, data_csv, load_file_path, "PMFBY.PROPOSAL")
        if up_status != 1:
            raise Exception(up_status)

        # *****Insert feeback in to S3*****
        buf = io.BytesIO()
        feedback_df.to_csv(buf, encoding='utf-8', index=False)
        buf.seek(0)
        s3.put_object(Body=buf, Bucket='sdg-01',
                      Key=s3_feedback_path + str(upload_log_id) + "/Feedback.csv")

        with conn.cursor() as cur:
            query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 1, PROCESS_END_TIME=now(), FEEDBACK_FILE = 'Feedback.csv', SUCCESS = "+str(no_of_records_success)+", FAILED = " + \
                str(no_of_records_failed) + \
                "  where id = "+str(upload_log_id)+";"
            print("Query: ", query)
            cur.execute(query)
            conn.commit()

        pool.release(conn)
        gc.collect()
        print(upload_log_id, "2", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

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


def pending_task(uploaded_by: int, configid: int, upload_task_id: int, season: str, product: str, year: int, state: str, method: str, state_code: int):
    """
    This method creates a new instance of "Sarus_thread" class and starts the new thread
    """
    try:

        new_task = SARUS_thread(uploaded_by, configid, upload_task_id,
                                season, product, year, state, method, state_code)
        new_task.start()
        new_task.join(7200)
        new_task.raise_exception()
        new_task.join()

        return
    except Exception as e:
        print("Exception Occurred for task id ", upload_task_id)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        return


@pmfby.route('/check_process', methods=['GET'])
def check_process():

    try:
        try:
            conn = pymysql.connect(host=endpoint, user=username, password=password, database=database_name, ssl={
                                   "true": True}, connect_timeout=5, cursorclass=pymysql.cursors.DictCursor, autocommit=True)
        except pymysql.MySQLError as e:
            print(
                "SCHEDULER ERROR: Unexpected error: Could not connect to MySQL instance.")
            print(e)
            # return
            gc.collect()
            message = {
                'status': 502,
                'message': "ERROR: Unexpected error: Could not connect to MySQL instance.",
            }
            respone = jsonify(message)
            respone.status_code = 502
            return respone

        upload_task_id = -1
        with conn.cursor() as cur:
            query = "select * from PMFBY.FILEUPLOAD_LOGS where FEEDBACK_AVAILABLE=0"
            # print(query)
            cur.execute(query)
            rc = cur.rowcount
            result = cur.fetchall()
            # print(result)
            # print(type(result))

        if rc > 0:
            print("SCHEDULER: Total ongoing task: ", rc)
            killed_process = []
            ongoing_process = []
            for x in result:
                upload_task_id = x['ID']
                # print("time diff of " + str(upload_task_id) + ": ", (datetime.now() - x['PROCESS_START_TIME']).total_seconds())

                if (datetime.now() - x['PROCESS_START_TIME']).total_seconds() > 7200:
                    killed_process.append(upload_task_id)
                    query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE=2, PROCESS_END_TIME=now(), REMARK = 'Something went wrong', EXCEPTION = 'Process taking too long', where ID = " + \
                        str(x['ID'])
                    # print(query)
                    with conn.cursor() as cur:
                        cur.execute(query)
                        conn.commit()
                else:
                    ongoing_process.append(upload_task_id)

                conn.close()
                gc.collect()
                # return
                msg = "ONGOING PROCESS: " + \
                    ','.join([str(i) for i in ongoing_process])
                if len(killed_process) > 0:
                    msg += " | KILLED PROCESS: " + \
                        ','.join([str(i) for i in killed_process])
                message = {
                    'status': 200,
                    'message':  msg,
                    'task_id': -1
                }
                print("SCHEDULER Response: ", message['message'])
                response = jsonify(message)
                response.status_code = 200
                return response

        else:
            # change product name
            query = """select a.UPLOADED_BY, a.CONFIGID, a.ID, a.METHOD, b.SEASON, b.PRODUCT, b.YEAR, b.STATE, c.STATE_CODE
            from PMFBY.FILEUPLOAD_LOGS a
            left join PMFBY.PRODUCT_CONFIG b on a.CONFIGID=b.CONFIGID
            left join PMFBY.STATE_MST c on c.STATE_NAME=b.STATE
            where FEEDBACK_AVAILABLE=-1
            order by ID
            limit 1"""
            # print(query)
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchone()
            # print(result)
            if result != None:
                upload_task_id = result['ID']
                print("SCHEDULER Process Started:  ", upload_task_id)
                # change product name
                thread = Thread(target=pending_task, kwargs={'uploaded_by': result['UPLOADED_BY'], 'configid': result['CONFIGID'], 'upload_task_id': result['ID'], 'season': result[
                                'SEASON'], 'product': result['PRODUCT'], 'year': result['YEAR'], 'state': result['STATE'], 'method': result['METHOD'], 'state_code': result['STATE_CODE']})
                thread.start()

                # if result['METHOD'] == "PMFBY_Underwriting_Creation":
                # 	pmfby.uw_upload_thread(result['UPLOADED_BY'], result['CONFIGID'], result['ID'], result['SEASON'], result['PRODUCT'], result['YEAR'], result['STATE'])
                # elif result['METHOD'] == "PMFBY_Notification_Creation":
                # 	pmfby.notifiction_upload_thread(result['UPLOADED_BY'], result['CONFIGID'], result['ID'])

                # pool.release(conn)
                conn.close()
                gc.collect()
                # return
                message = {
                    'status': 200,
                    'message':  "PROCESS STARTED",
                    'task_id': upload_task_id
                }
                response = jsonify(message)
                response.status_code = 200
                return response

            else:
                print("SCHEDULER No process pending")
                conn.close()
                gc.collect()
                # return
                message = {
                    'status': 200,
                    'message':  "NO PROCESS PENDING",
                    'task_id': -1
                }
                response = jsonify(message)
                response.status_code = 200
                return response

    except Exception as e:
        print("SCHEDULER Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        print("SCHEDULER Exception: ", e)
        conn.close()
        gc.collect()
        message = {
            'status': 502,
            'message': str(e),
            'task_id': upload_task_id
        }
        respone = jsonify(message)
        respone.status_code = 502
        return respone


@pmfby.route('/download_uploaded_files', methods=['GET'])
def download_uploaded_files():
    global pool
    print(pool)
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
    print(conn)
    try:
        id = request.args.get('id')
        print("downloading files for upload_task_id ", id)
        s = io.BytesIO()
        filename = 'file_upload_' + str(id)+'.zip'
        zf = zipfile.ZipFile(s, 'w')
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket='sdg-01', Prefix=s3_user_file_path + str(id) + '/')
        for page in page_iterator:
            if page['KeyCount'] > 0:
                # print(upload_log_id, page)
                for file in page['Contents']:
                    # print(upload_log_id, file)
                    key = file['Key']
                    if '.' not in key:
                        continue
                    data = s3.get_object(Bucket='sdg-01', Key=key)
                    zf.writestr(str(key[key.rindex("/")+1:]),
                                data.get('Body').read())

        zf.close()
        s.seek(0)
        gc.collect()
        print("done")
        respone = flask.make_response(flask.send_file(
            s, mimetype='application/zip', as_attachment=True, download_name=filename))
        respone.status_code = 200
        return respone

    except Exception as e:
        print("Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        conn.close()
        gc.collect()
        message = {
            'status': 502,
            'message': str(e)
        }
        respone = jsonify(message)
        respone.status_code = 502
        return respone


@pmfby.route('/pmfby_bulk_upload', methods=['POST'])
def pmfby_bulk_upload():
    print("---------pmfby_bulk_upload-----------")
    global pool
    try:
        print(pool)
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
        print(conn)
        print(request.form)
        user_id = request.form.get('userid')
        configid = request.form.get('configid')
        if configid is None:
            configid = 'NULL'
        # configid = int(configid)
        method = request.form.get('method')
        filetype = request.form.get('file_type')
        uploaded_file = request.files.getlist("file[]")

        if len(uploaded_file) == 0:
            message = {
                'status': 502,
                'message':  "No files detected"
            }
            respone = jsonify(message)
            respone.status_code = 502
            pool.release(conn)
            gc.collect()
            return respone

        print(user_id, configid, method, uploaded_file)

        # Insert starting Entry into file upload table
        with conn.cursor() as cur:
            query = "INSERT INTO PMFBY.FILEUPLOAD_LOGS(METHOD,UPLOADED_BY,CONFIGID,UPLOADED_ON,FILETYPE) values('" + str(method) + "','"+str(user_id)+"',"+str(configid) + \
                ",now(),'" + str(filetype) + "');"
            print("Query: ", query)
            cur.execute(query)
            conn.commit()
            upload_log_id = cur.lastrowid
        print("upload_log", upload_log_id)

        # Save user file to S3
        for index in range(len(uploaded_file)):
            print(index, uploaded_file[index])
            # filename.append(uploaded_file[index].filename)
            s3.put_object(Body=uploaded_file[index].read(), Bucket='sdg-01',
                          Key=s3_user_file_path + str(upload_log_id) + "/" + str(uploaded_file[index].filename))
            uploaded_file[index].seek(0)

        with conn.cursor() as cur:
            query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = -1 where id = " + \
                str(upload_log_id)+";"
            # print("Query: ", query)
            cur.execute(query)
            conn.commit()
        message = {
            'status': 200,
            'message':  "1"
        }
        response = jsonify(message)
        response.status_code = 200
        return response

    except Exception as e:
        print("Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        e_str = str(e).replace('"', '\\"')
        e_str = e_str.replace("'", "\\'")
        with conn.cursor() as cur:
            query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, REMARK = '" + \
                str(e_str)+"' where id = "+str(upload_log_id)+";"
            # print("Query: ", query)
            cur.execute(query)
            conn.commit()
        message = {
            'status': 502,
            'message':  str(e)
        }
        respone = jsonify(message)
        respone.status_code = 502
        pool.release(conn)
        gc.collect()
        return respone


@pmfby.route('/pmfby_template_generation', methods=['GET'])
def pmfby_template_generation():
    global notif_row, uw_row, pv_coll_row, tid_coll_row, bsb_uw_row, intimation_row
    try:

        method = request.args.get('method')
        filetype = request.args.get('file_type')
        print("Template Generation :::: ", method, filetype)
        if method in ("PMFBY_Notification_Creation", "RWBCIS_Notification_Creation"):
            filename = "Scheme_Notification_Template"
            worksheet_name = "Notification_Data"
            columns = notif_row
        elif method in ("PMFBY_Underwriting_Creation", "RWBCIS_Underwriting_Creation"):
            filename = "Scheme_Underwriting_Template"
            worksheet_name = "Underwriting_Data"
            columns = uw_row
        elif method in ("PMFBY_Underwriting_Endorsement", "RWBCIS_Underwriting_Endorsement"):
            filename = "Scheme_UW_Endorsement_Template"
            worksheet_name = "UW_Endorse_Data"
            columns = uw_row
        elif method == "PV_Collection_Creation":
            filename = "PV_Collection_Creation_Template"
            worksheet_name = "PVCollection_Data"
            columns = pv_coll_row
        elif method == "TID_Collection_Creation":
            filename = "TID_Collection_Creation_Template"
            worksheet_name = "TIDCollection_Data"
            columns = tid_coll_row
        elif method == "BSB_Notification_Creation":
            filename = "BSB_Notification_Template"
            worksheet_name = "BSB_Notification_Data"
            columns = bsb_notif_row_kharif
        elif method == "BSB_Underwriting_Creation":
            filename = "BSB_Underwriting_Template"
            worksheet_name = "BSB_Underwriting_Data"
            columns = bsb_uw_row
        elif method == "ILA_Intimation_Creation":
            filename = "Intimation_Template"
            worksheet_name = "ILA_Intimation_Data"
            columns = intimation_row
        else:
            return "Unrecognized template type"

        if filetype == 'excel':
            template_path = file_path + "/" + filename + ".xlsx"
            try:
                os.remove(template_path)
            except:
                pass

            wb = xlsxwriter.Workbook(template_path, {'in_memory': True})

            worksheet = wb.add_worksheet(worksheet_name)
            font_style = wb.add_format({'bold': True})

            row_num = 0
            for col_num in range(len(columns)):
                worksheet.write(row_num, col_num, columns[col_num], font_style)
                worksheet.set_column(row_num, col_num, 20)
            wb.close()

        elif filetype == 'csv':
            template_path = file_path + "/" + filename + ".csv"
            try:
                os.remove(template_path)
            except:
                pass

            with open(template_path, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(columns)

        else:
            return "Invalid file type"

        ##### Sending Response #####
        return send_file(template_path)

    except Exception as e:
        print("Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        message = {
            'status': 502,
            'message': str(e)
        }
        respone = jsonify(message)
        respone.status_code = 502
        return respone

# @pmfby.route('/pmfby_underwriting_endorsement', methods=['POST'])


def endorsement_upload_thread(user_id: int, configid: int, upload_log_id: int, season: str, product: str, year: int, state: str, state_code: int):
    """
    This method fetches endorsement file from AWS S3 based on the "upload_log_id" 
    and does endorsement of existing data in Proposal table
    """

    try:
        time_now = datetime.now()
        global uw_row, pool
        year = str(year)
        print(user_id, configid, upload_log_id,
              season, product, year, state, state_code)
        mmyy = time_now.strftime("%m") + time_now.strftime("%Y")[-2:]
        season_code = season[:2] + str(year[-2:]) + str(int(year[-2:])+1)
        account_tag = "/".join([str(i)
                                for i in [product, state_code, season_code, mmyy]])
        # print("before sleep")
        # time.sleep(20)
        # print("after sleep")
        print("1", upload_log_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        # print(pool)
        # *****creating connection*****
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
        # print(conn)
        dictionary = {"SCHEME NAME": "PRODUCT_NAME",	"YEAR": "YEAR",	"SEASON": "SEASON",	"STATE NAME": "STATE",	"APPLICATION ID": "FARMER_APPLICATION_ID",	"FARMER ID": "FARMER_ID",	"FARMER NAME": "FARMER_NAME",	"FARMER ID TYPE": "FARMER_ID_TYPE",	"FARMER ID NUMBER": "FARMER_ID_NO",	"RELATIVE NAME": "FARMER_RELATIVE_NAME",	"RELATIVE'S RELATION": "FARMER_RELATIVE_RELATION",	"FARMER MOBILE": "FARMER_MOBILE",	"AGE": "FARMER_AGE",	"GENDER": "FARMER_GENDER",	"COMMUNITY": "FARMER_CASTE",	"ADDRESS": "FARMER_ADDRESS",	"STATE OF RESIDENCE": "FARMER_L2_NAME",	"STATE CODE (RESIDENCE)": "FARMER_L2_CODE",	"DISTRICT OF RESIDENCE": "FARMER_L3_NAME",	"DISTRICT CODE (RESIDENCE)": "FARMER_L3_CODE",	"SUB DISTRICT NAME OF RESIDENCE": "FARMER_L4_NAME",	"SUB DISTRICT CODE (RESIDENCE)": "FARMER_L4_CODE",	"VILLAGE OF RESIDENCE": "FARMER_L7_NAME",	"VILLAGE CODE(RESIDENCE)": "FARMER_L7_CODE",	"PINCODE": "FARMER_PINCODE",	"BANK NAME": "BANK_NAME",	"BANK ID": "BANK_ID",	"BRANCH NAME": "BANK_BRANCH_NAME",	"BRANCH ID": "BANK_BRANCH_ID",	"IFSC": "IFSC",	"FARMER TYPE": "FARMER_TYPE",	"ACCOUNT OWNERSHIP TYPE": "ACCOUNT_OWNERSHIP_TYPE",	"ACCOUNT NUMBER": "FARMER_BANK_ACCOUNT_NO",	"FARMER CATEGORY": "FARMER_CATEGORY",	"NATURE OF FARMER": "FARMER_NATURE",	"FARMER ACCOUNT TYPE": "FARMER_ACCOUNT_TYPE",	"INSURANCE COMPANY NAME": "INSURANCE_COMPANY_NAME",	"INUSRANCE COMPANY CODE": "INSURANCE_COMPANY_CODE",	"CROP NAME": "FARMER_CROP_NAME",	"CROP CODE": "CROP_CODE",	"CROP STATE NAME": "CROP_L2_NAME",	"CROP STATE CODE": "CROP_L2_CODE",	"CROP DISTRICT NAME": "CROP_L3_NAME",	"CROP DISTRICT CODE": "CROP_L3_CODE",	"IU LEVEL": "INSURANCE_UNIT_LEVEL",
                      "IU NAME": "INSURANCE_UNIT_NAME",	"IU CODE": "INSURANCE_UNIT_CODE",	"CROP VILLAGE NAME": "CROP_L7_NAME",	"CROP VILLAGE CODE": "CROP_L7_CODE",	"LAND SURVEY NUMBER": "LAND_SURVEY_NUMBER",	"LAND SUBDIVISION NUMBER": "LAND_SUB_DIVISION_NUMBER",	"AREA INSURED": "FARMER_AREA_INSURED",	"FARMER SHARE": "FARMER_SHAREOF_PREMIUM",	"STATE SHARE": "STATE_SHAREOF_PREMIUM",	"GOI SHARE": "GOI_SHAREOF_PREMIUM",	"GROSS PREMIUM": "GROSS_PREMIUM",	"SUM INSURED": "SUM_INSURED",	"MIX CROP": "MIX_CROP",	"COMBINED AREA INSURED": "COMBINED_INSURED_AREA",	"PREMIUM DEBIT DATE": "PREMIUM_DEBIT_DATE",	"SOWING DATE": "SOWING_DATE",	"CUT OF DATE": "CUT_OFF_DATE",	"INDEMNITY LEVEL": "INDEMNITY",	"APPLICATION STATUS": "APPLICATION_STATUS",	"APPLICATION SOURCE": "APPLICATION_SOURCE",	"UTR/TRANSACTION NUMBER": "UTR_TRANSACTION_NUMBER",	"UTR/TRANSACTION AMOUNT": "UTR_TRANSACTION_AMOUNT",	"UTR/TRANSACTION DATE": "UTR_TRANSACTION_DATE",	"UTR/TRANSACTION MODE": "UTR_TRANSACTION_MODE",	"LEVEL4NAME": "CROP_L4_NAME",	"LEVEL4CODE": "CROP_L4_CODE",	"LEVEL5NAME": "CROP_L5_NAME",	"LEVEL5CODE": "CROP_L5_CODE",	"LEVEL6NAME": "CROP_L6_NAME",	"LEVEL6CODE": "CROP_L6_CODE",	"TIMESTAMP DATA GENERATION": "TIMESTAMP_DATA_GENERATION",	"CREATED BANK NAME": "CREATED_BANK_NAME",	"CREATED BANK CODE": "CREATED_BANK_ID",	"CREATED BRANCH NAME": "CREATED_BANK_BRANCH_NAME",	"CREATED BRANCH CODE": "CREATED_BANK_BRANCH_ID",	"CREATED USER NAME": "CREATED_USER_NAME",	"CSC ID": "CSC_ID",	"APPLICATION CREATED DATE": "APPLICATION_CREATED_DATE",	"LAND RECORD FARMER NAME": "LAND_RECORD_FARMER_NAME",	"LAND RECORD AREA": "LAND_RECORD_AREA"}

        duplicate_list = ['YEAR', 'SEASON', 'STATE', 'FARMER_NAME',
                          'CROP_CODE', 'LAND_SURVEY_NUMBER', 'LAND_SUB_DIVISION_NUMBER']
        de_duplicate_list = ['FARMER_ID', 'IFSC']
        notif_match_list = ['CROP_CODE']
        valuesToBeChecked = {
            'STATE': [state],
            'YEAR': [year],
            'SEASON': [season],
            'FARMER_APPLICATION_ID': None,
            'FARMER_ID': None,
            'FARMER_NAME': None,
            'FARMER_ID_TYPE': None,
            'FARMER_ID_NO': None,
            'FARMER_RELATIVE_NAME': None,
            'FARMER_MOBILE': None,
            'BANK_NAME': None,
            'BANK_ID': None,
            'BANK_BRANCH_NAME': None,
            'BANK_BRANCH_ID': None,
            'IFSC': None,
            'FARMER_TYPE': None,
            'FARMER_BANK_ACCOUNT_NO': None,
            'FARMER_CATEGORY': None,
            'CROP_CODE': None,
            'FARMER_CROP_NAME': None,
            'INSURANCE_UNIT_LEVEL': None,
            'INSURANCE_UNIT_CODE': None,
            'LAND_SURVEY_NUMBER': None,
            'FARMER_AREA_INSURED': None,
            'SUM_INSURED': None,
            'GROSS_PREMIUM': None,
            'FARMER_SHAREOF_PREMIUM': None,
            'STATE_SHAREOF_PREMIUM': None,
            'GOI_SHAREOF_PREMIUM': None,
            'APPLICATION_STATUS': ['APPROVED', 'PENDING FOR APPROVAL(DECLARATION)', 'REJECTED', 'CSC REVERTED'],
            'APPLICATION_SOURCE': None,
        }

        numericColumns = {
            'FARMER_AREA_INSURED': 'isnumeric',
            'SUM_INSURED': 'isnumeric',
            'GROSS_PREMIUM': 'isnumeric',
            'FARMER_SHAREOF_PREMIUM': 'isnumeric',
            'STATE_SHAREOF_PREMIUM': 'isnumeric',
            'GOI_SHAREOF_PREMIUM': 'isnumeric',
        }
        status_update_pending = {
            'APPLICATION_STATUS': []
        }
        calc_fields = '''
            CALC_SUM_INSURED = NOTIF_SI * FARMER_AREA_INSURED
            CALC_FARMER_SHAREOF_PREMIUM = NOTIF_SI * FARMER_AREA_INSURED * FARMER_SHARE_PCT / 100
            CALC_STATE_SHAREOF_PREMIUM = NOTIF_SI * FARMER_AREA_INSURED * STATE_SHARE_PCT / 100
            CALC_GOI_SHAREOF_PREMIUM = NOTIF_SI * FARMER_AREA_INSURED * GOI_SHARE_PCT / 100
            CALC_GROSS_PREMIUM = NOTIF_SI * FARMER_AREA_INSURED * PREMIUM_RATE_PCT / 100
        '''
        fin_check = {
            'SUM_INSURED': {'CALC_SUM_INSURED': [1, 2]},
            'FARMER_SHAREOF_PREMIUM': {'CALC_FARMER_SHAREOF_PREMIUM': [1, 2]},
            'STATE_SHAREOF_PREMIUM': {'CALC_STATE_SHAREOF_PREMIUM': [1, 2]},
            'GOI_SHAREOF_PREMIUM': {'CALC_GOI_SHAREOF_PREMIUM': [1, 2]},
            'GROSS_PREMIUM': {'CALC_GROSS_PREMIUM': [1, 2]},
            'CALC_GROSS_PREMIUM_INDIR': {'GROSS_PREMIUM': [1, 2]}
        }
        time_get = time_now.strftime("%Y%m%d%H%M%S")
        updated_date_time = time_now.strftime("%Y-%m-%d %H:%M:%S")
        file_name = 'endorse_'+str(user_id)+"_"+str(time_get)

        # *****Get Data in form of DF*****
        csv_file_data1 = upload_thread_DF_init(conn, upload_log_id, uw_row, exclude_columns=[
                                               'FILENAME'], optional_columns={'DUPLICATE': ''}, header_rename=dictionary)
        if type(csv_file_data1) != pd.DataFrame:
            pool.release(conn)
            gc.collect()
            return

        csv_file_data1 = csv_file_data1.apply(
            lambda x: x.astype(str).str.upper().str.strip())

        csv_file_data1 = csv_file_data1.replace('\\\\', '\\\\\\\\', regex=True)

        csv_file_data1[['CROP_CODE', 'CROP_L2_CODE', 'CROP_L3_CODE', 'CROP_L4_CODE', 'CROP_L5_CODE', 'CROP_L6_CODE', 'CROP_L7_CODE']] = csv_file_data1[['CROP_CODE', 'CROP_L2_CODE', 'CROP_L3_CODE', 'CROP_L4_CODE', 'CROP_L5_CODE', 'CROP_L6_CODE', 'CROP_L7_CODE']].apply(
            lambda x: x.str.lstrip('0'))

        csv_file_data1 = csv_file_data1.replace('', None)

        total_records = csv_file_data1.shape[0]

        feedback_file = []

        column_name_excel_template = csv_file_data1.columns.tolist()
        # print(upload_log_id, column_name_excel_template)

        # *****Calculate GROSS PREMIUM*****
        csv_file_data1['CALC_GROSS_PREMIUM_INDIR'] = pd.to_numeric(csv_file_data1['FARMER_SHAREOF_PREMIUM'], errors='coerce') + pd.to_numeric(
            csv_file_data1['STATE_SHAREOF_PREMIUM'], errors='coerce') + pd.to_numeric(csv_file_data1['GOI_SHAREOF_PREMIUM'], errors='coerce')

        # *****Fetching existing proposal data*****
        proposal_query = "SELECT * FROM PMFBY.PROPOSAL WHERE PROPOSAL_STATUS<>'REJECTED' AND CONFIGID=" + \
            str(configid)
        # print("query-------------", proposal_query)
        # exist_data = cx.read_sql(read_connection_string, "SELECT * FROM PMFBY.PROPOSAL WHERE PROPOSAL_STATUS<>'REJECTED' AND CONFIGID='" +
        #                          str(configid) + "'limit 10 ", partition_on="PROPOSAL_ID", partition_num=10)
        exist_data = pd.read_sql(proposal_query,
                                 read_connection_string, dtype={'PROPOSAL_ID': str, 'FARMER_APPLICATION_ID': str})

        exist_data = exist_data.apply(
            lambda x: x.astype(str).str.upper().str.strip())

        # *****Fetching Notification data*****
        get_notif_query = 'SELECT NOTIFICATION_ID,CROP_CODE,L3_NAME_CODE as "CROP_L3_CODE",L4_NAME_CODE as "CROP_L4_CODE",L5_NAME_CODE as "CROP_L5_CODE",L6_NAME_CODE as "CROP_L6_CODE",L7_NAME_CODE as "CROP_L7_CODE", FARMER_SHARE_PCT, GOI_SHARE_PCT, STATE_SHARE_PCT, PREMIUM_RATE_PCT, SUM_INSURED as "NOTIF_SI" FROM PMFBY.NOTIFICATION where CONFIGID=' + \
            str(configid)

        # notif_data = cx.read_sql(
        #     read_connection_string, get_notif_query, partition_on="NOTIFICATION_ID", partition_num=10, dtype={'CROP_CODE': str, 'CROP_L3_CODE': str, 'CROP_L4_CODE': str, 'CROP_L5_CODE': str, 'CROP_L6_CODE': str, 'CROP_L7_CODE': str})
        notif_data = pd.read_sql(get_notif_query,
                                 read_connection_string, dtype={'CROP_CODE': str, 'CROP_L3_CODE': str, 'CROP_L4_CODE': str, 'CROP_L5_CODE': str, 'CROP_L6_CODE': str, 'CROP_L7_CODE': str})

        notif_data = notif_data.apply(
            lambda x: x.astype(str).str.upper().str.strip())

        notif_data[['FARMER_SHARE_PCT', 'GOI_SHARE_PCT', 'STATE_SHARE_PCT', 'PREMIUM_RATE_PCT', 'NOTIF_SI']] = notif_data[[
            'FARMER_SHARE_PCT', 'GOI_SHARE_PCT', 'STATE_SHARE_PCT', 'PREMIUM_RATE_PCT', 'NOTIF_SI']].apply(pd.to_numeric, errors='coerce')

        csv_file_data1['PROPOSAL_STATUS'] = ""
        csv_file_data1['REMARKS'] = ""
        csv_file_data1['UPLOAD_TASK_ID'] = upload_log_id

        csv_file_data1['UPDATED_DATE'] = updated_date_time
        csv_file_data1['UPDATE_BY'] = user_id

        # *****Fetching hierarchy data*****
        with conn.cursor() as cur:
            query = "select * from PMFBY.LOCATION_HIERARCHY where STATE_CODE=" + \
                str(state_code)+";"
            print("Query: ", query)
            cur.execute(query)
            hierarchy_data = cur.fetchone()

        print("Hierarchy Data: ", hierarchy_data)

        # *****Validity check based on hierarchy data*****
        if hierarchy_data['L3'] is not None:
            valuesToBeChecked['CROP_L3_NAME'] = None
            valuesToBeChecked['CROP_L3_CODE'] = None
            numericColumns['CROP_L3_CODE'] = 'isinteger'
            notif_match_list.append('CROP_L3_CODE')

        if hierarchy_data['L4'] is not None:
            valuesToBeChecked['CROP_L4_NAME'] = None
            valuesToBeChecked['CROP_L4_CODE'] = None
            numericColumns['CROP_L4_CODE'] = 'isinteger'
            notif_match_list.append('CROP_L4_CODE')

        if hierarchy_data['L5'] is not None:
            valuesToBeChecked['CROP_L5_NAME'] = None
            valuesToBeChecked['CROP_L5_CODE'] = None
            numericColumns['CROP_L5_CODE'] = 'isinteger'
            notif_match_list.append('CROP_L5_CODE')

        if hierarchy_data['L6'] is not None:
            valuesToBeChecked['CROP_L6_NAME'] = None
            valuesToBeChecked['CROP_L6_CODE'] = None
            numericColumns['CROP_L6_CODE'] = 'isinteger'
            notif_match_list.append('CROP_L6_CODE')

        if hierarchy_data['L7'] is not None:
            valuesToBeChecked['CROP_L7_NAME'] = None
            valuesToBeChecked['CROP_L7_CODE'] = None
            numericColumns['CROP_L7_CODE'] = 'isinteger'
            notif_match_list.append('CROP_L7_CODE')

        notif_data = notif_data[['NOTIFICATION_ID', 'FARMER_SHARE_PCT', 'GOI_SHARE_PCT',
                                 'STATE_SHARE_PCT', 'PREMIUM_RATE_PCT', 'NOTIF_SI'] + notif_match_list]

        dfObject = CheckValidity(csv_file_data1)

        # *****Removing duplicate APPLICATION_ID from self*****
        print("removing duplicate APPLICATION_ID form self")

        exec_status = dfObject.removeDuplicateSelf(
            ['FARMER_APPLICATION_ID'], 'DUPLICATE FARMER_APPLICATION_ID IN FILE', False, duplicate_list + de_duplicate_list)
        if exec_status != True:
            raise Exception(exec_status)

        # *****Removing duplicate insurances from self*****
        print("removing duplicate list from self")
        exec_status = dfObject.removeDuplicateSelf(
            duplicate_list, 'DUPLICATE INSURANCE IN FILE', False, de_duplicate_list)
        if exec_status != True:
            raise Exception(exec_status)

        # *****Check if Proposal is present*****
        print("Check if Proposal is present")
        exec_status = dfObject.matchFromDf(
            exist_data[['FARMER_APPLICATION_ID', 'PROPOSAL_ID']], ['FARMER_APPLICATION_ID'], "PROPOSAL NOT FOUND")
        if exec_status != True:
            raise Exception(exec_status)

        temp = dfObject.getValidDf()

        # *****Check Duplicate from DB where Application ID is different*****
        print("Check Duplicate from DB where Application ID is different")
        tempObject = CheckValidity(exist_data)
        exec_status = tempObject.removeDuplicateFromDf(dfObject.getValidDf(), [
            'FARMER_APPLICATION_ID'], 'FARMER_APPLICATION_ID ALREADY EXISTS IN DATABASE')
        if exec_status != True:
            raise Exception(exec_status)

        exec_status = dfObject.removeDuplicateFromDf(
            tempObject.getValidDf(), duplicate_list, 'INSURANCE ALREADY EXISTS IN DATABASE')
        if exec_status != True:
            raise Exception(exec_status)

        # *****Check if Notification is present*****
        print("notification check")
        exec_status = dfObject.matchFromDf(
            notif_data, notif_match_list, "NOTIFICATION NOT FOUND")
        if exec_status != True:
            raise Exception(exec_status)

        # *****Data validation for necessary columns and datatypes*****
        exec_status = dfObject.checkValue(numericColumns)
        if exec_status != True:
            raise Exception(exec_status)

        exec_status = dfObject.checkValue(valuesToBeChecked)
        if exec_status != True:
            raise Exception(exec_status)

        # *****Add Calculated Fields*****
        exec_status = dfObject.addColumnFromFormula(calc_fields, "valid")
        if exec_status != True:
            raise Exception(exec_status)
        
        if state != 'ASSAM':
            exec_status = dfObject.checkValue(fin_check)
            if exec_status != True:
                raise Exception(exec_status)
        
        # *****Add accounting tag*****

        exec_status = dfObject.updateValueFromText(
            {"STATE": [state]}, "equal", {"ACCOUNT_TAG": account_tag})
        if exec_status != True:
            raise Exception(exec_status)

       # *****Add Party ID*****
        exec_status = dfObject.updateValueFromColumn({"APPLICATION_SOURCE": ["BANK", "CBS"]}, "equal", {
            "PARTY_ID": ["CREATED_BANK_ID", "CREATED_BANK_BRANCH_ID", "SEASON", "YEAR"]}, "/")
        if exec_status != True:
            raise Exception(exec_status)

        exec_status = dfObject.updateValueFromColumn({"APPLICATION_SOURCE": ["BANK", "CBS"]}, "not equal", {
            "PARTY_ID": ["FARMER_APPLICATION_ID", "SEASON", "YEAR"]}, "/")
        if exec_status != True:
            raise Exception(exec_status)

        # *****Update PROPOSAL_STATUS*****
        exec_status = dfObject.updateValueFromText({"APPLICATION_STATUS": [
                                                    'PENDING FOR APPROVAL(DECLARATION)', 'REJECTED', 'CSC REVERTED']}, 'equal', {"PROPOSAL_STATUS": "PENDING FOR APPROVAL"})
        if exec_status != True:
            raise Exception(exec_status)

        exec_status = dfObject.updateValueFromText(
            {"APPLICATION_STATUS": ['APPROVED']}, 'equal', {"PROPOSAL_STATUS": "APPROVED"})
        if exec_status != True:
            raise Exception(exec_status)

       # *****Valid Data*****
        data_csv = dfObject.getValidDf()

        # feedback_df.to_excel(file_path + "/" + "feedback.xlsx")
        # data_csv.to_excel(file_path + "/" + "data.xlsx")

        print("valid data: ", data_csv.shape[0])
        no_of_records_success = data_csv.shape[0]
        no_of_records_failed = (total_records-no_of_records_success)

        start_time = datetime.now()
        # load_file_path = file_path + "/" + file_name + ".csv"

        # *****Invalid Data and Feedback*****
        feedback_df = dfObject.getInvalidDf()
        feedback_df['FARMER_APPLICATION_ID'] = "'" + \
            feedback_df['FARMER_APPLICATION_ID']
        feedback_df = feedback_df.drop(
            ['CALC_GROSS_PREMIUM_INDIR', 'PROPOSAL_STATUS', 'REMARKS', 'UPLOAD_TASK_ID', 'UPDATED_DATE', 'UPDATE_BY', 'DUPLICATE', 'BUSINESS_LOGIC_VALID', 'NOTIFICATION_ID', 'FARMER_SHARE_PCT', 'GOI_SHARE_PCT', 'STATE_SHARE_PCT', 'PREMIUM_RATE_PCT', 'NOTIF_SI', 'PROPOSAL_ID'], errors='ignore', axis=1
        )

        # ************UPDATION OF PROPOSAL TABLE****************
        if no_of_records_success != 0:

            # date = time_now.strftime("%m")
            # acc_tag_year = time_now.strftime("%Y")
            # if state == 'UTTAR PRADESH':
            # 	state_code = "UTP"
            # else:
            # 	state_code = str(state)[0:3]

            # data_csv['ACCOUNT_TAG'] = str(product+'/'+season+'/'+acc_tag_year+'/'+state_code+'/'+date).replace(' ', '')
            data_csv['CONFIGID'] = configid
            pd.to_numeric(data_csv["FARMER_AREA_INSURED"])
            # pd.to_numeric(data_csv["ACF"])

            # data_csv[ 'AREA_AFTER_ACF'] = data_csv[ 'FARMER_AREA_INSURED']
            data_csv = data_csv.drop(
                ['CALC_GROSS_PREMIUM_INDIR', 'FILENAME', 'BUSINESS_LOGIC_VALID', 'REMARK', 'FARMER_SHARE_PCT', 'GOI_SHARE_PCT', 'STATE_SHARE_PCT', 'PREMIUM_RATE_PCT', 'NOTIF_SI', 'DUPLICATE'], errors='ignore', axis=1)

            first_column = data_csv.pop('PROPOSAL_ID')
            data_csv.insert(0, 'PROPOSAL_ID', first_column)

            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE PMFBY.TEMP_PROPOSAL_TABLE")
                conn.commit()
            # *****Upload csv file to db*****
            load_file_path = file_path + "/" + file_name + ".csv"
            up_status = upload_file_to_DB(
                conn, data_csv, load_file_path, "PMFBY.TEMP_PROPOSAL_TABLE")
            if up_status != 1:
                raise Exception(up_status)

            update_query = """
            UPDATE PMFBY.PROPOSAL p
            JOIN PMFBY.TEMP_PROPOSAL_TABLE tmp ON p.PROPOSAL_ID = tmp.PROPOSAL_ID
            SET % s
            """
            set_clause = ', '.join(
                [f"p.{col}=tmp.{col}" for col in data_csv.columns[1:]])
            update_query = update_query % set_clause
            # print(update_query)
            with conn.cursor() as cur:
                cur.execute(update_query)
                conn.commit()

            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE PMFBY.TEMP_PROPOSAL_TABLE")
                conn.commit()

        # *************INSERTING DATA IN ENDORSEMENT TABLE***************

            data_csv.insert(0, 'ENDORSEMENT_ID', None)

            # *****Upload csv file to db*****
            load_file_path = file_path + "/" + file_name + ".csv"
            up_status = upload_file_to_DB(
                conn, data_csv, load_file_path, "PMFBY.ENDORSEMENT")
            if up_status != 1:
                raise Exception(up_status)

            # *************INSERTING DATA IN ENDORSEMENT HISTORY***************
            query = "select ENDORSEMENT_ID, PROPOSAL_ID from PMFBY.ENDORSEMENT where UPLOAD_TASK_ID=" + \
                str(upload_log_id)
            print(query)
            # endorsed_data = cx.read_sql(read_connection_string, query,
            #                             partition_on="ENDORSEMENT_ID", partition_num=10)

            endorsed_data = pd.read_sql(query,
                                        connection_string, dtype={'PROPOSAL_ID': str, 'ENDORSEMENT_ID': str})
            endorsed_data = endorsed_data.apply(
                lambda x: x.astype(str))
            if (data_csv.shape[0] != endorsed_data.shape[0]):
                with conn.cursor() as cur:
                    query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Something went wrong', EXCEPTION = 'Shape Mismatch' where id = " + \
                        str(upload_log_id)+";"
                    print("Query: ", query)
                    cur.execute(query)
                    conn.commit()
                pool.release(conn)
                gc.collect()
                return

            newObj = CheckValidity(tempObject.getInvalidDf())
            exec_status = newObj.matchFromDf(
                endorsed_data[['ENDORSEMENT_ID', 'PROPOSAL_ID']], ['PROPOSAL_ID'], '')
            if exec_status != True:
                raise Exception(exec_status)

            exec_status = newObj.matchFromDf(
                data_csv[['PROPOSAL_ID']], ['PROPOSAL_ID'], '')
            if exec_status != True:
                raise Exception(exec_status)

            positive_data = newObj.getValidDf()
            eid = positive_data.pop('ENDORSEMENT_ID')
            positive_data.insert(0, 'ENDORSEMENT_HISTORY_ID', None)
            positive_data.insert(1, 'ENDORSEMENT_ID', eid)

            negative_data = positive_data.copy()

            negative_col = ["FARMER_AREA_INSURED", "FARMER_SHAREOF_PREMIUM", "STATE_SHAREOF_PREMIUM",
                            "GOI_SHAREOF_PREMIUM", "GROSS_PREMIUM", "SUM_INSURED", "COMBINED_INSURED_AREA", "LAND_RECORD_AREA"]
            negative_data[negative_col] = negative_data[negative_col].apply(
                pd.to_numeric, errors='coerce')

            negative_data[["FARMER_AREA_INSURED", "FARMER_SHAREOF_PREMIUM", "STATE_SHAREOF_PREMIUM", "GOI_SHAREOF_PREMIUM", "GROSS_PREMIUM", "SUM_INSURED", "COMBINED_INSURED_AREA", "LAND_RECORD_AREA"]] = negative_data[["FARMER_AREA_INSURED", "FARMER_SHAREOF_PREMIUM", "STATE_SHAREOF_PREMIUM", "GOI_SHAREOF_PREMIUM", "GROSS_PREMIUM", "SUM_INSURED", "COMBINED_INSURED_AREA", "LAND_RECORD_AREA"]].applymap(lambda
                                                                                                                                                                                                                                                                                                                                                                                                                   x: -x)
            negative_data['UPLOAD_TASK_ID'] = upload_log_id

            negative_data['UPDATED_DATE'] = updated_date_time

            negative_data['UPDATE_BY'] = user_id

            endorsement_history = pd.concat([positive_data, negative_data])

            # *****Upload csv file to db*****
            load_file_path = file_path + "/" + file_name + ".csv"
            up_status = upload_file_to_DB(
                conn, endorsement_history, load_file_path, "PMFBY.ENDORSEMENT_HISTORY")
            if up_status != 1:
                raise Exception(up_status)

        end_time = datetime.now()
        print(upload_log_id, "dump................................____",
              end_time-start_time)

        # *****Insert feeback in to S3*****
        buf = io.BytesIO()
        feedback_df.to_csv(buf, encoding='utf-8', index=False)
        buf.seek(0)

        s3.put_object(Body=buf, Bucket='sdg-01',
                      Key=s3_feedback_path + str(upload_log_id) + "/Feedback.csv")

        with conn.cursor() as cur:
            query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 1, PROCESS_END_TIME=now(), FEEDBACK_FILE = 'Feedback.csv', SUCCESS = "+str(no_of_records_success)+", FAILED = " + \
                str(no_of_records_failed) + \
                "  where id = "+str(upload_log_id)+";"
            print("Query: ", query)
            cur.execute(query)
            conn.commit()

        pool.release(conn)
        gc.collect()
        print(upload_log_id, "2", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

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


def endorsement_upload_thread_old(user_id, configid, upload_log_id, season, product, year, state, state_code):
    print("inside endorsement_upload_thread")
    global euw_row, pool
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
        file_name = 'uwe_'+str(user_id)+"_" + \
            str(configid)+"_"+str(time_get)+".csv"

        # table_name = 'AIC_STAGING.'+str(file_name[:-5])
        # print(table_name)

        csv_file_data1 = read_files_from_s3(
            upload_log_id, filetype, s3_user_file_path)
        print("file type------------", type(csv_file_data1))
        if type(csv_file_data1) is dict:
            with conn.cursor() as cur:
                e_str = str(csv_file_data1["error"]).replace('"', '\\"')
                e_str = e_str.replace("'", "\\'")
                query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Error while reading file. " + e_str + "' where id = " + \
                        str(upload_log_id)+";"
                print("Query: ", query)
                cur.execute(query)
                conn.commit()
            pool.release(conn)
            gc.collect()
            return

        print(upload_log_id, csv_file_data1.shape[0])

        # # CHeck Headers if it dosen't Match
        columns = euw_row
        headers = list(csv_file_data1.columns.values)
        headers.remove('FILENAME')
        # print(headers)

        try:
            # if(not set(headers).issubset(set(columns))):
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
                query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = '" + \
                        str(e_str)+"' where id = "+str(upload_log_id)+";"
                print("Query: ", query)
                cur.execute(query)
                conn.commit()
            pool.release(conn)
            gc.collect()
            return

        # ------------------------------------------------------------------------------

        updated_date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        dictionary = {'APPLICATION ID*': 'FARMER_APPLICATION_ID', 'FARMER ID': 'FARMER_ID', 'FARMER NAME': 'FARMER_NAME',
                      'RELATIVE NAME': 'FARMER_RELATIVE_NAME', "RELATIVE'S RELATION": 'FARMER_RELATIVE_RELATION', 'FARMER MOBILE': 'FARMER_MOBILE', 'AGE': 'FARMER_AGE', 'GENDER': 'FARMER_GENDER', 'COMMUNITY': 'FARMER_CASTE',
                      'ADDRESS': 'FARMER_ADDRESS', 'STATE OF RESIDENCE': 'FARMER_L2_NAME', 'STATE CODE (RESIDENCE)': 'FARMER_L2_CODE', 'DISTRICT OF RESIDENCE': 'FARMER_L3_NAME', 'DISTRICT CODE (RESIDENCE)': 'FARMER_L3_CODE', 'SUB DISTRICT NAME OF RESIDENCE': 'FARMER_L4_NAME',
                      'SUB DISTRICT CODE (RESIDENCE)': 'FARMER_L4_CODE', 'VILLAGE OF RESIDENCE': 'FARMER_L7_NAME', 'VILLAGE CODE (RESIDENCE)': 'FARMER_L7_CODE', 'PINCODE': 'FARMER_PINCODE', 'BANK NAME': 'BANK_NAME', 'BANK ID': 'BANK_ID', 'BRANCH NAME': 'BANK_BRANCH_NAME',
                      'BRANCH ID': 'BANK_BRANCH_ID', 'IFSC': 'IFSC', 'FARMER TYPE': 'FARMER_TYPE', 'ACCOUNT OWNERSHIP TYPE': 'ACCOUNT_OWNERSHIP_TYPE', 'ACCOUNT NUMBER': 'FARMER_BANK_ACCOUNT_NO', 'FARMER CATEGORY': 'FARMER_CATEGORY', 'NATURE OF FARMER': 'FARMER_NATURE',
                      'FARMER ACCOUNT TYPE': 'FARMER_ACCOUNT_TYPE', 'CROP NAME': 'FARMER_CROP_NAME', 'CROP CODE': 'CROP_CODE', 'LAND SURVEY NUMBER': 'LAND_SURVEY_NUMBER', 'LAND SUBDIVISION NUMBER': 'LAND_SUB_DIVISION_NUMBER', 'AREA INSURED': 'FARMER_AREA_INSURED', 'MIX CROP': 'MIX_CROP', 'COMBINED AREA INSURED': 'COMBINED_INSURED_AREA', 'PREMIUM DEBIT DATE': 'PREMIUM_DEBIT_DATE', 'SOWING DATE': 'SOWING_DATE',
                      'CUT OF DATE': 'CUT_OFF_DATE', 'APPLICATION SOURCE': 'APPLICATION_SOURCE', 'UTR/TRANSACTION NUMBER': 'UTR_TRANSACTION_NUMBER', 'UTR/TRANSACTION AMOUNT': 'UTR_TRANSACTION_AMOUNT',
                      'UTR/TRANSACTION DATE': 'UTR_TRANSACTION_DATE', 'UTR/TRANSACTION MODE': 'UTR_TRANSACTION_MODE', 'CREATED BANK NAME': 'CREATED_BANK_NAME', 'CREATED BANK CODE': 'CREATED_BANK_ID', 'CREATED BRANCH NAME': 'CREATED_BANK_BRANCH_NAME', 'CREATED BRANCH CODE': 'CREATED_BANK_BRANCH_ID',
                      'CREATED USER NAME': 'CREATED_USER_NAME', 'CSC ID': 'CSC_ID', 'APPLICATION CREATED DATE': 'APPLICATION_CREATED_DATE', 'LAND RECORD FARMER NAME': 'LAND_RECORD_FARMER_NAME', 'LAND RECORD AREA': 'LAND_RECORD_AREA'}
        csv_file_data1.rename(columns=dictionary, inplace=True)

        csv_file_data1[['CROP_CODE']].apply(lambda x: x.str.lstrip('0'))
        csv_file_data1 = csv_file_data1.replace('', None)

        id_list = "("
        for index in csv_file_data1["FARMER_APPLICATION_ID"].index:
            id_list += "'" + \
                str(csv_file_data1["FARMER_APPLICATION_ID"][index]) + "', "
        id_list = id_list[:-2] + ")"

        proposal_query = "SELECT PROPOSAL_ID,FARMER_APPLICATION_ID,NOTIFICATION_ID,FARMER_AREA_INSURED AS EXIST_AREA_INSURED, FARMER_CROP_NAME, CROP_CODE, CROP_L3_NAME, CROP_L3_CODE,CROP_L4_NAME, CROP_L4_CODE, CROP_L5_NAME, CROP_L5_CODE,CROP_L6_NAME, CROP_L6_CODE,CROP_L7_NAME, CROP_L7_CODE FROM PMFBY.PROPOSAL WHERE FARMER_APPLICATION_ID IN " + \
            str(id_list) + " AND PROPOSAL_STATUS<>'REJECTED' AND CONFIGID='" + \
            str(configid) + "' ; "
        print("--------proposal_query---------", proposal_query)
        exist_data_proposal = cx.read_sql(
            read_connection_string, proposal_query, partition_on="PROPOSAL_ID", partition_num=100)

        proposal_data = exist_data_proposal.copy()
        proposal_data = proposal_data.drop(
            ["FARMER_CROP_NAME", "CROP_CODE"], axis=1, errors='ignore')
        csv_file_data1 = pd.merge(csv_file_data1, proposal_data, on=[
            'FARMER_APPLICATION_ID'], how='left', suffixes=('_csv', '_db'))

        noti_query = "SELECT NOTIFICATION_ID,CROP_NAME AS FARMER_CROP_NAME ,CROP_CODE, L3_NAME AS CROP_L3_NAME,L3_NAME_CODE AS CROP_L3_CODE, L4_NAME AS CROP_L4_NAME,L4_NAME_CODE AS CROP_L4_CODE, L5_NAME AS CROP_L5_NAME,L5_NAME_CODE AS CROP_L5_CODE, L6_NAME AS CROP_L6_NAME,L6_NAME_CODE AS CROP_L6_CODE, L7_NAME AS CROP_L7_NAME,L7_NAME_CODE AS CROP_L7_CODE , PREMIUM_RATE_PCT, FARMER_SHARE_PCT, GOI_SHARE_PCT, STATE_SHARE_PCT, SUM_INSURED FROM PMFBY.NOTIFICATION WHERE CONFIGID=" + \
            str(configid) + "; "
        print("--------test_query_noti---------", noti_query)
        exist_data_noti = cx.read_sql(
            read_connection_string, noti_query, partition_on="NOTIFICATION_ID", partition_num=100)

        csv_file_data1['CONFIGID'] = configid
        csv_file_data1['UPLOAD_TASK_ID'] = upload_log_id
        csv_file_data1['UPDATE_BY'] = user_id
        csv_file_data1['UPDATED_DATE'] = updated_date_time
        csv_file_data1['REMARK'] = ''
        # csv_file_data1['DATA_VALIDATION'] = 0
        # csv_file_data1['APP_ID_MATCH'] = True
        csv_file_data1 = csv_file_data1.apply(
            lambda x: x.astype(str).str.upper().str.strip())
        exist_data_noti['NOTIFICATION_ID'] = exist_data_noti['NOTIFICATION_ID'].astype(
            str)
        csv_file_data1 = csv_file_data1.replace('NONE', None)

        duplicate_list = ['FARMER_APPLICATION_ID']
        de_duplicate_list = ['FARMER_APPLICATION_ID']

        csv_file_data1.loc[csv_file_data1['FARMER_CROP_NAME'].notnull() & csv_file_data1['CROP_CODE'].isnull(),
                           'REMARK'] = csv_file_data1['REMARK'] + ' CROP CODE cannot be null, '

        csv_file_data1.loc[csv_file_data1['CROP_CODE'].notnull() & csv_file_data1['FARMER_CROP_NAME'].isnull(),
                           'REMARK'] = csv_file_data1['REMARK'] + ' CROP NAME cannot be null, '

        dfObject = CheckValidity(csv_file_data1)

        # *****Check DUPLICATE FARMER APPLICATION ID IN FILE*****
        exec_status = dfObject.removeDuplicateSelf(
            duplicate_list, "DUPLICATE FARMER APPLICATION ID IN FILE", True, de_duplicate_list)
        if exec_status != True:
            raise Exception(exec_status)

        # *****Check if FARMER_APPLICATION_ID is present*****

        exec_status = dfObject.matchFromDf(
            exist_data_proposal['FARMER_APPLICATION_ID'], duplicate_list, "FARMER_APPLICATION_ID NOT FOUND")
        if exec_status != True:
            raise Exception(exec_status)

        # *****isnumeric check*****
        valuesToBeChecked = {
            'FARMER_AREA_INSURED': 'isnumeric',
        }

        exec_status = dfObject.checkValue(valuesToBeChecked)
        if exec_status != True:
            raise Exception(exec_status)
        # print("---------dfObject--------",
        #       dfObject)

        csv_data = dfObject.getValidDf()
        # print("headers-----------", csv_data.columns.values)
        # -----------------non_financial---------------------
        # print("---------csv_data--------",
        #       csv_data.shape[0])
        non_financial = csv_data.copy()
        # print("---------non_financial.shape000000--------",
        #   non_financial.shape[0])
        non_financial['NF_CHECK'] = csv_data['FARMER_AREA_INSURED'].isnull(
        ) & csv_data['FARMER_CROP_NAME'].isnull() & csv_data['CROP_CODE'].isnull()
        # print("---------non_financial.shape111111--------",
        #   non_financial.shape[0])
        # print("-------NF_CHECK-------", non_financial['NF_CHECK'])
        non_financial = non_financial[non_financial['NF_CHECK'] == True]
        # print("non_financial headers-----------", non_financial.columns.values)
        non_financial = non_financial.drop(
            ['NF_CHECK'], axis=1, errors='ignore')
        # print("---------non_financial.shape2222222222--------",
        #   non_financial.shape[0])

        #-----------------area check-------------------#
        csv_area = csv_data.copy()
        csv_area['AREA_CHECK'] = csv_data['FARMER_AREA_INSURED'].notnull(
        ) & csv_data['FARMER_CROP_NAME'].isnull() & csv_data['CROP_CODE'].isnull()
        csv_area = csv_area[csv_area['AREA_CHECK'] == True]
        area = pd.merge(csv_area, exist_data_noti[['NOTIFICATION_ID', 'PREMIUM_RATE_PCT', 'FARMER_SHARE_PCT', 'GOI_SHARE_PCT',
                                                   'STATE_SHARE_PCT', 'SUM_INSURED']], on=['NOTIFICATION_ID'], how='inner', suffixes=('_csv', '_db'))
        area = area.drop(['AREA_CHECK'], axis=1, errors='ignore')

        #-----------------crop check-------------------#
        csv_crop = csv_data.copy()
        csv_crop['CROP_CHECK'] = csv_data['FARMER_CROP_NAME'].notnull(
        ) & csv_data['CROP_CODE'].notnull()

        csv_crop = csv_crop[csv_crop['CROP_CHECK'] == True]

        csv_crop.loc[csv_crop['FARMER_AREA_INSURED'].isnull(
        ), 'FARMER_AREA_INSURED'] = csv_crop['EXIST_AREA_INSURED']

        crop = pd.merge(csv_crop, exist_data_noti, on=["FARMER_CROP_NAME", "CROP_CODE", "CROP_L3_NAME", "CROP_L3_CODE", "CROP_L4_NAME", "CROP_L4_CODE",
                                                       "CROP_L5_NAME", "CROP_L5_CODE", "CROP_L6_NAME", "CROP_L6_CODE", "CROP_L7_NAME", "CROP_L7_CODE"], how='inner', suffixes=('_csv', '_db'))
        crop['NOTIFICATION_ID_csv'] = crop['NOTIFICATION_ID_db']
        crop.rename(
            columns={'NOTIFICATION_ID_csv': 'NOTIFICATION_ID'}, inplace=True)

        csv_file_data1['APP_ID_MATCH'] = csv_file_data1['FARMER_APPLICATION_ID'].isin(
            crop['FARMER_APPLICATION_ID'])

        conditions = [
            (csv_file_data1['APP_ID_MATCH'] == True & csv_file_data1['FARMER_CROP_NAME'].notnull(
            ) & csv_file_data1['CROP_CODE'].notnull())
        ]

        values = ['']
        values2 = [' Notification not found for CROP']
        csv_file_data1['REMARK'] = csv_file_data1['REMARK'] + \
            np.select(conditions, values, values2)
        csv_file_data1.loc[csv_file_data1['REMARK']
                           == '', 'REMARK'] = 'SUCCESS'
        crop = crop.drop(['NOTIFICATION_ID_db', 'CROP_CHECK'],
                         axis=1, errors='ignore')

        frames = [area, crop]
        financial = pd.concat(frames, ignore_index=True)

        financial['new_premium'] = financial['PREMIUM_RATE_PCT'].astype(float)*financial['SUM_INSURED'].astype(
            float)*financial['FARMER_AREA_INSURED'].astype(float)
        financial['new_sumInsured'] = financial['SUM_INSURED'].astype(
            float)*financial['FARMER_AREA_INSURED'].astype(float)
        financial['new_farmerShare'] = financial['FARMER_SHARE_PCT'].astype(float)*financial['SUM_INSURED'].astype(
            float)*financial['FARMER_AREA_INSURED'].astype(float)
        financial['new_goiShare'] = financial['GOI_SHARE_PCT'].astype(float)*financial['SUM_INSURED'].astype(
            float)*financial['FARMER_AREA_INSURED'].astype(float)
        financial['new_stateShare'] = financial['STATE_SHARE_PCT'].astype(float)*financial['SUM_INSURED'].astype(
            float)*financial['FARMER_AREA_INSURED'].astype(float)

        frames = [financial, non_financial]
        main_data = pd.concat(frames, ignore_index=True)

        # main_data = main_data[
        #     (main_data["DB_APPLICATION_ID"] == True) & (
        #         main_data["XL_APPLICATION_DUP"] == False)
        # ]
        main_data = main_data.drop(
            ["APP_ID_MATCH", "EXIST_AREA_INSURED", "REMARK", "CROP_L3_NAME", "CROP_L3_CODE", "CROP_L4_NAME", "CROP_L4_CODE", "CROP_L5_NAME", "CROP_L5_CODE", "CROP_L6_NAME", "CROP_L6_CODE", "CROP_L7_NAME", "CROP_L7_CODE", 'PREMIUM_RATE_PCT', 'FARMER_SHARE_PCT', 'GOI_SHARE_PCT', 'STATE_SHARE_PCT', 'SUM_INSURED', 'FILENAME', 'BUSINESS_LOGIC_VALID'], axis=1, errors='ignore')

        main_data = main_data.dropna(axis=1, how='all')

        # -----------account tag----------------
        # print(user_id, configid, upload_log_id,season, product, year, state, state_code)
        year = str(year)
        mmyy = time_now.strftime("%m") + time_now.strftime("%Y")[-2:]
        season_code = season[:2] + str(year[-2:]) + str(int(year[-2:])+1)
        main_data['ACCOUNT_TAG'] = "/".join([str(i)
                                             for i in [product, state_code, season_code, mmyy]])

        # print(main_data['ACCOUNT_TAG'])

        db_columns = main_data.copy()
        db_columns = db_columns.drop(
            ['new_premium', 'new_sumInsured', 'new_farmerShare', 'new_goiShare', 'new_stateShare'], axis=1, errors='ignore')
        db_columns['PRODUCT_NAME'] = product
        db_columns['YEAR'] = year
        db_columns['SEASON'] = season
        db_columns['STATE'] = state
        # print("df columns------------", db_columns.columns.values)
        # -----------------------endorsement insert query -------------------------
        db_columns.to_sql(name='endorsement', schema="PMFBY", con=connection_string,
                          if_exists='append', index=False, chunksize=1000, method='multi')

        main_data = main_data.drop(
            ['PROPOSAL_ID'], axis=1, errors='ignore')
        # print("---------main_data.shape--------", main_data.shape[0])
        if (main_data.shape[0] != 0):
            # -----------------account tag--------------

            headers = list(main_data.columns.values)
            # print("main data--------------", headers)
            with conn.cursor() as cur:
                # -----------------------proposal update query-------------------------
                # query = '''update PMFBY.PROPOSAL set
                # '''
                query = '''update PMFBY.proposal set
                '''
                id_list = "("
                for item in headers:
                    # print(item)

                    if (str(item) == 'new_premium' or str(item) == 'new_sumInsured' or str(item) == 'new_farmerShare' or str(item) == 'new_goiShare' or str(item) == 'new_stateShare'):
                        continue
                    if str(item) != 'FARMER_APPLICATION_ID':
                        query += '''
                        '''+str(item)+''' = case FARMER_APPLICATION_ID
                        '''
                    if str(item) == 'FARMER_AREA_INSURED':
                        premium = '''
                        GROSS_PREMIUM = case FARMER_APPLICATION_ID
                        '''
                        farmer_share = '''
                        FARMER_SHAREOF_PREMIUM = case FARMER_APPLICATION_ID
                        '''
                        state_share = '''
                        STATE_SHAREOF_PREMIUM = case FARMER_APPLICATION_ID
                        '''
                        goi_share = '''
                        GOI_SHAREOF_PREMIUM = case FARMER_APPLICATION_ID
                        '''
                        sum_insured = '''
                        SUM_INSURED = case FARMER_APPLICATION_ID
                        '''

                    for index in main_data.index:
                        if str(item) == 'FARMER_APPLICATION_ID':
                            id = main_data[str(item)][index]
                            # print("--id--",id)
                            id_list += "'" + \
                                str(main_data[str(item)][index]) + "', "
                            continue

                        if main_data[str(item)][index] != None:
                            if str(item) == 'FARMER_AREA_INSURED':

                                premium += '''when "''' + str(main_data['FARMER_APPLICATION_ID'][index]) + '''" then "''' + str(financial['new_premium'][index]) + '''"
                                '''
                                farmer_share += '''when "''' + str(main_data['FARMER_APPLICATION_ID'][index]) + '''" then "''' + str(financial['new_farmerShare'][index]) + '''"
                                '''
                                state_share += '''when "''' + str(main_data['FARMER_APPLICATION_ID'][index]) + '''" then "''' + str(financial['new_stateShare'][index]) + '''"
                                '''
                                goi_share += '''when "''' + str(main_data['FARMER_APPLICATION_ID'][index]) + '''" then "''' + str(financial['new_goiShare'][index]) + '''"
                                '''
                                sum_insured += '''when "''' + str(main_data['FARMER_APPLICATION_ID'][index]) + '''" then "''' + str(financial['new_sumInsured'][index]) + '''"
                                '''

                            query += '''when "''' + str(main_data['FARMER_APPLICATION_ID'][index]) + '''" then "''' + str(main_data[str(item)][index]) + '''"
                            '''

                    if str(item) != 'FARMER_APPLICATION_ID':
                        query += '''else ''' + str(item) + '''
                        end,'''
                    if str(item) == 'FARMER_AREA_INSURED':
                        premium += '''else GROSS_PREMIUM
                        end,'''
                        farmer_share += '''else FARMER_SHAREOF_PREMIUM
                        end,'''
                        state_share += '''else STATE_SHAREOF_PREMIUM
                        end,'''
                        goi_share += '''else GOI_SHAREOF_PREMIUM
                        end,'''
                        sum_insured += '''else SUM_INSURED
                        end,'''

                        query += premium + farmer_share + state_share + \
                            goi_share + sum_insured
                query = query[:-1]
                id_list = id_list[:-2] + ")"
                query += '''
                where FARMER_APPLICATION_ID in ''' + id_list
                # print(query)

                # -----------------------endorsement_history insert query -------------------------

                query2 = '''INSERT INTO pmfby.endorsement_history(PROPOSAL_ID,PRODUCT_NAME,YEAR,SEASON,STATE,FARMER_APPLICATION_ID
                            ,FARMER_ID ,FARMER_NAME ,FARMER_ID_TYPE ,FARMER_ID_NO ,FARMER_RELATIVE_NAME,FARMER_RELATIVE_RELATION
                            ,FARMER_MOBILE,FARMER_AGE,FARMER_GENDER,FARMER_CASTE,FARMER_ADDRESS,FARMER_L2_NAME,FARMER_L2_CODE,FARMER_L3_NAME
                            ,FARMER_L3_CODE ,FARMER_L4_NAME,FARMER_L4_CODE,FARMER_L7_NAME,FARMER_L7_CODE,FARMER_PINCODE,BANK_NAME
                            ,BANK_ID,BANK_BRANCH_NAME,BANK_BRANCH_ID,IFSC,FARMER_TYPE,ACCOUNT_OWNERSHIP_TYPE,FARMER_BANK_ACCOUNT_NO
                            ,FARMER_CATEGORY,FARMER_NATURE,FARMER_ACCOUNT_TYPE,INSURANCE_COMPANY_NAME,INSURANCE_COMPANY_CODE,FARMER_CROP_NAME
                            ,CROP_CODE,CROP_L2_NAME,CROP_L2_CODE,CROP_L3_NAME,CROP_L3_CODE,INSURANCE_UNIT_LEVEL,INSURANCE_UNIT_NAME
                            ,INSURANCE_UNIT_CODE,CROP_L7_NAME,CROP_L7_CODE,LAND_SURVEY_NUMBER,LAND_SUB_DIVISION_NUMBER,FARMER_AREA_INSURED
                            ,FARMER_SHAREOF_PREMIUM,STATE_SHAREOF_PREMIUM,GOI_SHAREOF_PREMIUM,GROSS_PREMIUM,SUM_INSURED,MIX_CROP,COMBINED_INSURED_AREA
                            ,PREMIUM_DEBIT_DATE,SOWING_DATE,CUT_OFF_DATE,INDEMNITY,APPLICATION_STATUS,APPLICATION_SOURCE ,UTR_TRANSACTION_NUMBER
                            ,UTR_TRANSACTION_AMOUNT,UTR_TRANSACTION_DATE,UTR_TRANSACTION_MODE,CROP_L4_NAME,CROP_L4_CODE,CROP_L5_NAME
                            ,CROP_L5_CODE,CROP_L6_NAME,CROP_L6_CODE,TIMESTAMP_DATA_GENERATION,CREATED_BANK_NAME,CREATED_BANK_ID
                            ,CREATED_BANK_BRANCH_NAME,CREATED_BANK_BRANCH_ID,CREATED_USER_NAME,CSC_ID,APPLICATION_CREATED_DATE
                            ,LAND_RECORD_FARMER_NAME ,LAND_RECORD_AREA,PROPOSAL_STATUS,REMARK,UPLOAD_TASK_ID
                            ,UPDATED_DATE,UPDATE_BY,NOTIFICATION_ID,ACCOUNT_TAG,CONFIGID,ENDORSEMENT_REFERENCE_ID)

                            select a.PROPOSAL_ID,a.PRODUCT_NAME,a.YEAR,a.SEASON,a.STATE,a.FARMER_APPLICATION_ID,a.FARMER_ID,a.FARMER_NAME,
                            a.FARMER_ID_TYPE,a.FARMER_ID_NO,a.FARMER_RELATIVE_NAME,a.FARMER_RELATIVE_RELATION,a.FARMER_MOBILE,a.FARMER_AGE,
                            a.FARMER_GENDER,a.FARMER_CASTE,a.FARMER_ADDRESS,a.FARMER_L2_NAME,a.FARMER_L2_CODE,a.FARMER_L3_NAME,a.FARMER_L3_CODE,
                            a.FARMER_L4_NAME,a.FARMER_L4_CODE,a.FARMER_L7_NAME,a.FARMER_L7_CODE,a.FARMER_PINCODE,a.BANK_NAME,a.BANK_ID,
                            a.BANK_BRANCH_NAME,a.BANK_BRANCH_ID,a.IFSC,a.FARMER_TYPE,a.ACCOUNT_OWNERSHIP_TYPE,a.FARMER_BANK_ACCOUNT_NO,
                            a.FARMER_CATEGORY,a.FARMER_NATURE,a.FARMER_ACCOUNT_TYPE,a.INSURANCE_COMPANY_NAME,a.INSURANCE_COMPANY_CODE,
                            a.FARMER_CROP_NAME,a.CROP_CODE,a.CROP_L2_NAME,a.CROP_L2_CODE,a.CROP_L3_NAME,a.CROP_L3_CODE,a.INSURANCE_UNIT_LEVEL,
                            a.INSURANCE_UNIT_NAME,a.INSURANCE_UNIT_CODE,a.CROP_L7_NAME,a.CROP_L7_CODE,a.LAND_SURVEY_NUMBER,
                            a.LAND_SUB_DIVISION_NUMBER,a.FARMER_AREA_INSURED,a.FARMER_SHAREOF_PREMIUM,a.STATE_SHAREOF_PREMIUM,
                            a.GOI_SHAREOF_PREMIUM,a.GROSS_PREMIUM,a.SUM_INSURED,a.MIX_CROP,a.COMBINED_INSURED_AREA,a.PREMIUM_DEBIT_DATE,
                            a.SOWING_DATE,a.CUT_OFF_DATE,a.INDEMNITY,a.APPLICATION_STATUS,a.APPLICATION_SOURCE,a.UTR_TRANSACTION_NUMBER,
                            a.UTR_TRANSACTION_AMOUNT,a.UTR_TRANSACTION_DATE,a.UTR_TRANSACTION_MODE,a.CROP_L4_NAME,a.CROP_L4_CODE,a.CROP_L5_NAME,
                            a.CROP_L5_CODE,a.CROP_L6_NAME,a.CROP_L6_CODE,a.TIMESTAMP_DATA_GENERATION,a.CREATED_BANK_NAME,a.CREATED_BANK_ID,
                            a.CREATED_BANK_BRANCH_NAME,a.CREATED_BANK_BRANCH_ID,a.CREATED_USER_NAME,a.CSC_ID,a.APPLICATION_CREATED_DATE,
                            a.LAND_RECORD_FARMER_NAME,a.LAND_RECORD_AREA,a.PROPOSAL_STATUS,a.REMARKS,a.UPLOAD_TASK_ID,a.UPDATED_DATE,a.UPDATE_BY,
                            a.NOTIFICATION_ID,a.ACCOUNT_TAG,a.CONFIGID,b.ENDORSEMENT_ID FROM pmfby.proposal as a
                            inner join pmfby.endorsement as b on a.PROPOSAL_ID=b.PROPOSAL_ID
                            where b.UPLOAD_TASK_ID = ''' + str(upload_log_id)
                print(query2)

                cur.execute(query2)
                conn.commit()
                # -----------------------endorsement negative entry query -------------------------
                query3 = '''INSERT INTO pmfby.endorsement_history(PROPOSAL_ID,PRODUCT_NAME,YEAR,SEASON,STATE,FARMER_APPLICATION_ID
                            ,FARMER_ID ,FARMER_NAME ,FARMER_ID_TYPE ,FARMER_ID_NO ,FARMER_RELATIVE_NAME,FARMER_RELATIVE_RELATION
                            ,FARMER_MOBILE,FARMER_AGE,FARMER_GENDER,FARMER_CASTE,FARMER_ADDRESS,FARMER_L2_NAME,FARMER_L2_CODE,FARMER_L3_NAME
                            ,FARMER_L3_CODE ,FARMER_L4_NAME,FARMER_L4_CODE,FARMER_L7_NAME,FARMER_L7_CODE,FARMER_PINCODE,BANK_NAME
                            ,BANK_ID,BANK_BRANCH_NAME,BANK_BRANCH_ID,IFSC,FARMER_TYPE,ACCOUNT_OWNERSHIP_TYPE,FARMER_BANK_ACCOUNT_NO
                            ,FARMER_CATEGORY,FARMER_NATURE,FARMER_ACCOUNT_TYPE,INSURANCE_COMPANY_NAME,INSURANCE_COMPANY_CODE,FARMER_CROP_NAME
                            ,CROP_CODE,CROP_L2_NAME,CROP_L2_CODE,CROP_L3_NAME,CROP_L3_CODE,INSURANCE_UNIT_LEVEL,INSURANCE_UNIT_NAME
                            ,INSURANCE_UNIT_CODE,CROP_L7_NAME,CROP_L7_CODE,LAND_SURVEY_NUMBER,LAND_SUB_DIVISION_NUMBER,FARMER_AREA_INSURED
                            ,FARMER_SHAREOF_PREMIUM,STATE_SHAREOF_PREMIUM,GOI_SHAREOF_PREMIUM,GROSS_PREMIUM,SUM_INSURED,MIX_CROP,COMBINED_INSURED_AREA
                            ,PREMIUM_DEBIT_DATE,SOWING_DATE,CUT_OFF_DATE,INDEMNITY,APPLICATION_STATUS,APPLICATION_SOURCE ,UTR_TRANSACTION_NUMBER
                            ,UTR_TRANSACTION_AMOUNT,UTR_TRANSACTION_DATE,UTR_TRANSACTION_MODE,CROP_L4_NAME,CROP_L4_CODE,CROP_L5_NAME
                            ,CROP_L5_CODE,CROP_L6_NAME,CROP_L6_CODE,TIMESTAMP_DATA_GENERATION,CREATED_BANK_NAME,CREATED_BANK_ID
                            ,CREATED_BANK_BRANCH_NAME,CREATED_BANK_BRANCH_ID,CREATED_USER_NAME,CSC_ID,APPLICATION_CREATED_DATE
                            ,LAND_RECORD_FARMER_NAME ,LAND_RECORD_AREA,PROPOSAL_STATUS,REMARK,UPLOAD_TASK_ID
                            ,UPDATED_DATE,UPDATE_BY,NOTIFICATION_ID,ACCOUNT_TAG,CONFIGID,ENDORSEMENT_REFERENCE_ID)

                            select a.PROPOSAL_ID,a.PRODUCT_NAME,a.YEAR,a.SEASON,a.STATE,a.FARMER_APPLICATION_ID,a.FARMER_ID,a.FARMER_NAME,
                            a.FARMER_ID_TYPE,a.FARMER_ID_NO,a.FARMER_RELATIVE_NAME,a.FARMER_RELATIVE_RELATION,a.FARMER_MOBILE,a.FARMER_AGE,
                            a.FARMER_GENDER,a.FARMER_CASTE,a.FARMER_ADDRESS,a.FARMER_L2_NAME,a.FARMER_L2_CODE,a.FARMER_L3_NAME,a.FARMER_L3_CODE,
                            a.FARMER_L4_NAME,a.FARMER_L4_CODE,a.FARMER_L7_NAME,a.FARMER_L7_CODE,a.FARMER_PINCODE,a.BANK_NAME,a.BANK_ID,
                            a.BANK_BRANCH_NAME,a.BANK_BRANCH_ID,a.IFSC,a.FARMER_TYPE,a.ACCOUNT_OWNERSHIP_TYPE,a.FARMER_BANK_ACCOUNT_NO,
                            a.FARMER_CATEGORY,a.FARMER_NATURE,a.FARMER_ACCOUNT_TYPE,a.INSURANCE_COMPANY_NAME,a.INSURANCE_COMPANY_CODE,
                            a.FARMER_CROP_NAME,a.CROP_CODE,a.CROP_L2_NAME,a.CROP_L2_CODE,a.CROP_L3_NAME,a.CROP_L3_CODE,a.INSURANCE_UNIT_LEVEL,
                            a.INSURANCE_UNIT_NAME,a.INSURANCE_UNIT_CODE,a.CROP_L7_NAME,a.CROP_L7_CODE,a.LAND_SURVEY_NUMBER,a.LAND_SUB_DIVISION_NUMBER,
                            a.FARMER_AREA_INSURED-2*a.FARMER_AREA_INSURED as FARMER_AREA_INSURED,
                            a.FARMER_SHAREOF_PREMIUM-2*a.FARMER_SHAREOF_PREMIUM as FARMER_SHAREOF_PREMIUM,
                            a.STATE_SHAREOF_PREMIUM-2*a.STATE_SHAREOF_PREMIUM as STATE_SHAREOF_PREMIUM,
                            a.GOI_SHAREOF_PREMIUM-2*a.GOI_SHAREOF_PREMIUM as GOI_SHAREOF_PREMIUM,
                            a.GROSS_PREMIUM-2*a.GROSS_PREMIUM as GROSS_PREMIUM,
                            a.SUM_INSURED-2*a.SUM_INSURED as SUM_INSURED,
                            a.MIX_CROP,a.COMBINED_INSURED_AREA,a.PREMIUM_DEBIT_DATE,a.SOWING_DATE,a.CUT_OFF_DATE,a.INDEMNITY,a.APPLICATION_STATUS,
                            a.APPLICATION_SOURCE,a.UTR_TRANSACTION_NUMBER,a.UTR_TRANSACTION_AMOUNT,a.UTR_TRANSACTION_DATE,a.UTR_TRANSACTION_MODE,
                            a.CROP_L4_NAME,a.CROP_L4_CODE,a.CROP_L5_NAME,a.CROP_L5_CODE,a.CROP_L6_NAME,a.CROP_L6_CODE,a.TIMESTAMP_DATA_GENERATION,
                            a.CREATED_BANK_NAME,a.CREATED_BANK_ID,a.CREATED_BANK_BRANCH_NAME,a.CREATED_BANK_BRANCH_ID,a.CREATED_USER_NAME,
                            a.CSC_ID,a.APPLICATION_CREATED_DATE,a.LAND_RECORD_FARMER_NAME,a.LAND_RECORD_AREA,a.PROPOSAL_STATUS,a.REMARKS,a.UPLOAD_TASK_ID,
                            a.UPDATED_DATE,a.UPDATE_BY,a.NOTIFICATION_ID,b.ACCOUNT_TAG,a.CONFIGID,
                            b.ENDORSEMENT_ID
                            FROM pmfby.proposal as a
                            inner join pmfby.endorsement as b on a.PROPOSAL_ID=b.PROPOSAL_ID
                            where b.UPLOAD_TASK_ID = ''' + str(upload_log_id)

                print(query3)
                cur.execute(query3)
                conn.commit()

                # print(query)
                cur.execute(query)
                conn.commit()

        # calculating number of records succeed and failed---------------------
        total_records = csv_file_data1.shape[0]
        print("total_records", total_records)
        no_of_records_success = main_data.shape[0]
        print("no_of_records_success", no_of_records_success)
        no_of_records_failed = (total_records-no_of_records_success)
        print("no_of_records_failed", no_of_records_failed)

        # Insert feeback in to S3--------------------------------------------

        # print("csv headers------------", csv_file_data1.columns.values)
        csv_file_data1 = csv_file_data1.drop(
            ["APP_ID_MATCH", "CROP_L3_NAME", "CROP_L3_CODE", "CROP_L4_NAME", "CROP_L4_CODE", "CROP_L5_NAME", "CROP_L5_CODE", "CROP_L6_NAME", "CROP_L6_CODE", "CROP_L7_NAME", "CROP_L7_CODE", "EXIST_AREA_INSURED", "NOTIFICATION_ID", "PROPOSAL_ID", "FILENAME", "BUSINESS_LOGIC_VALID", "DUP_ID"], axis=1, errors='ignore')
        buf = io.BytesIO()
        csv_file_data1.to_csv(buf, index=False)
        buf.seek(0)
        s3.put_object(Body=buf, Bucket='sdg-01',
                      Key=s3_feedback_path + str(file_name))

        # template_path = file_path + str(file_name)
        # csv_file_data1.to_csv(template_path, index=False)

        with conn.cursor() as cur:
            # query = "update FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 1, FEEDBACK_FILE = '" + \
            query = "update fileupload_logs set FEEDBACK_AVAILABLE = 1, FEEDBACK_FILE = '" + \
                str(file_name) + "', SUCCESS = "+str(no_of_records_success)+", FAILED = " + \
                str(no_of_records_failed) + \
                "  where id = "+str(upload_log_id)+";"
            print("Query: ", query)
            cur.execute(query)
            conn.commit()

        pool.release(conn)
        gc.collect()
        print("2", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return True

    except Exception as e:
        print("Exception: ", e)
        e_str = str(e).replace('"', '\\"')
        e_str = e_str.replace("'", "\\'")
        with conn.cursor() as cur:
            # query = "update FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, REMARK = '" + \
            query = "update fileupload_logs set FEEDBACK_AVAILABLE = 2, REMARK = '" + \
                str(e_str)+"' where id = "+str(upload_log_id)+";"
            print("Query: ", query)
            cur.execute(query)
            conn.commit()
        pool.release(conn)
        gc.collect()
    # return response


def pmfby_uw_report_thread(user_id, configid, reporting_id, season, product, year, state, params):
    """
    This method creates a report based on the "params" (which is a JSON String) and stores the same in AWS S3
    """
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

        # *****database entry for process start*****
        report_time = datetime.now()
        with conn.cursor() as cur:
            query = "update PMFBY.REPORTING_LOGS set REPORT_AVAILABLE=0, PROCESS_START_TIME='" + report_time.strftime("%Y-%m-%d %H:%M:%S") + "' where REPORTING_ID=" + \
                str(reporting_id)
            print(reporting_id, query)
            cur.execute(query)
            conn.commit()

        param_dict = json.loads(params)
        district_code = param_dict['district_code']
        district = param_dict['district']
        proposal_status = param_dict['proposal_status']
        crop_name = param_dict['crop_name']
        crop_list = ""

        # columns = ['PROPOSAL_ID', 'PRODUCT_NAME', 'YEAR', 'SEASON', 'STATE', 'FARMER_APPLICATION_ID', 'FARMER_ID', 'FARMER_NAME', 'FARMER_ID_TYPE', 'FARMER_ID_NO', 'FARMER_RELATIVE_NAME', 'FARMER_RELATIVE_RELATION', 'FARMER_MOBILE,FARMER_AGE', 'FARMER_GENDER',
        #            'FARMER_CASTE', 'FARMER_ADDRESS', 'FARMER_L2_NAME', 'FARMER_L2_CODE', 'FARMER_L3_NAME', 'FARMER_L3_CODE', 'FARMER_L4_NAME', 'FARMER_L4_CODE', 'FARMER_L7_NAME', 'FARMER_L7_CODE', 'FARMER_PINCODE', 'BANK_NAME', 'BANK_ID ', 'BANK_BRANCH_NAME ', 'BANK_BRANCH_ID',
        #            'IFSC ', 'FARMER_TYPE ', 'ACCOUNT_OWNERSHIP_TYPE', 'FARMER_BANK_ACCOUNT_NO', 'FARMER_CATEGORY', 'FARMER_NATURE', 'FARMER_ACCOUNT_TYPE', 'INSURANCE_COMPANY_NAME,INSURANCE_COMPANY_CODE', 'FARMER_CROP_NAME', 'CROP_CODE', 'CROP_L2_NAME', 'CROP_L2_CODE', 'CROP_L3_NAME ',
        #            'CROP_L3_CODE', 'INSURANCE_UNIT_LEVEL', 'INSURANCE_UNIT_NAME', 'INSURANCE_UNIT_CODE', 'CROP_L7_NAME', 'CROP_L7_CODE', 'LAND_SURVEY_NUMBER', 'LAND_SUB_DIVISION_NUMBER', 'FARMER_AREA_INSURED', 'FARMER_SHAREOF_PREMIUM', 'STATE_SHAREOF_PREMIUM', 'GOI_SHAREOF_PREMIUM',
        #            'GROSS_PREMIUM', 'SUM_INSURED', 'MIX_CROP', 'COMBINED_INSURED_AREA', 'PREMIUM_DEBIT_DATE', 'SOWING_DATE', 'CUT_OFF_DATE', 'INDEMNITY', 'APPLICATION_STATUS,APPLICATION_SOURCE', 'UTR_TRANSACTION_NUMBER', 'UTR_TRANSACTION_AMOUNT,UTR_TRANSACTION_DATE', 'UTR_TRANSACTION_MODE',
        #            'CROP_L4_NAME', 'CROP_L4_CODE', 'CROP_L5_NAME', 'CROP_L5_CODE', 'CROP_L6_NAME', 'CROP_L6_CODE', 'TIMESTAMP_DATA_GENERATION', 'CREATED_BANK_NAME', 'CREATED_BANK_ID', 'CREATED_BANK_BRANCH_NAME,CREATED_BANK_BRANCH_ID', 'CREATED_USER_NAME', 'CSC_ID', 'APPLICATION_CREATED_DATE',
        #            'LAND_RECORD_FARMER_NAME', 'LAND_RECORD_AREA', 'PROPOSAL_STATUS', 'REMARKS', 'UPLOAD_TASK_ID', 'UPDATED_DATE', 'UPDATE_BY', 'NOTIFICATION_ID', 'ACCOUNT_TAG', 'PARTY_ID', 'CONFIGID']

        query = '''select  PROPOSAL_ID ,PRODUCT_NAME ,YEAR ,SEASON ,STATE ,FARMER_APPLICATION_ID ,FARMER_ID ,FARMER_NAME ,FARMER_ID_TYPE ,FARMER_ID_NO ,FARMER_RELATIVE_NAME ,FARMER_RELATIVE_RELATION ,FARMER_MOBILE,FARMER_AGE ,FARMER_GENDER ,
        FARMER_CASTE ,FARMER_ADDRESS ,FARMER_L2_NAME ,FARMER_L2_CODE ,FARMER_L3_NAME ,FARMER_L3_CODE ,FARMER_L4_NAME ,FARMER_L4_CODE ,FARMER_L7_NAME ,FARMER_L7_CODE ,FARMER_PINCODE ,BANK_NAME ,BANK_ID  ,BANK_BRANCH_NAME  ,BANK_BRANCH_ID ,IFSC  ,FARMER_TYPE  ,
        ACCOUNT_OWNERSHIP_TYPE ,FARMER_BANK_ACCOUNT_NO ,FARMER_CATEGORY ,FARMER_NATURE ,FARMER_ACCOUNT_TYPE ,INSURANCE_COMPANY_NAME,INSURANCE_COMPANY_CODE ,FARMER_CROP_NAME ,CROP_CODE ,CROP_L2_NAME ,CROP_L2_CODE ,CROP_L3_NAME  ,CROP_L3_CODE ,INSURANCE_UNIT_LEVEL ,
        INSURANCE_UNIT_NAME ,INSURANCE_UNIT_CODE ,CROP_L7_NAME ,CROP_L7_CODE ,LAND_SURVEY_NUMBER ,LAND_SUB_DIVISION_NUMBER ,FARMER_AREA_INSURED ,FARMER_SHAREOF_PREMIUM ,STATE_SHAREOF_PREMIUM ,GOI_SHAREOF_PREMIUM ,GROSS_PREMIUM ,SUM_INSURED ,MIX_CROP ,COMBINED_INSURED_AREA ,
        PREMIUM_DEBIT_DATE ,SOWING_DATE ,CUT_OFF_DATE ,INDEMNITY ,APPLICATION_STATUS,APPLICATION_SOURCE ,UTR_TRANSACTION_NUMBER ,UTR_TRANSACTION_AMOUNT,UTR_TRANSACTION_DATE ,UTR_TRANSACTION_MODE ,CROP_L4_NAME ,CROP_L4_CODE ,CROP_L5_NAME ,CROP_L5_CODE ,CROP_L6_NAME ,CROP_L6_CODE ,
        TIMESTAMP_DATA_GENERATION ,CREATED_BANK_NAME ,CREATED_BANK_ID ,CREATED_BANK_BRANCH_NAME,CREATED_BANK_BRANCH_ID ,CREATED_USER_NAME ,CSC_ID ,APPLICATION_CREATED_DATE ,LAND_RECORD_FARMER_NAME ,LAND_RECORD_AREA ,PROPOSAL_STATUS ,REMARKS ,UPLOAD_TASK_ID ,UPDATED_DATE ,UPDATE_BY ,
        NOTIFICATION_ID ,ACCOUNT_TAG ,PARTY_ID ,CONFIGID 
        from PMFBY.PROPOSAL where configid=''' + str(configid) + '''
        and CROP_L3_CODE=''' + str(district_code)

        if proposal_status != '%':
            query += ' and PROPOSAL_STATUS="' + str(proposal_status) + '"'

        if crop_name != 'all':
            crop_list = "("
            for x in crop_name:
                crop_list += '"' + str(x) + '",'
            crop_list = crop_list[:-1] + ")"
            print("----crop_list----", crop_list)
            query += ' and FARMER_CROP_NAME in ' + str(crop_list)

        # if search_text:

        #     query += " and (CROP_L3_NAME like '%"+str(search_text)+"%' or CROP_L4_NAME like '%"+str(search_text)+"%' or CROP_L5_NAME like '%"+str(search_text)+"%' or CROP_L6_NAME like '%"+str(search_text)+"%' or CROP_L7_NAME like '%"+str(
        #         search_text)+"%' or SUM_INSURED like '%"+str(search_text)+"%' or GROSS_PREMIUM like '%"+str(search_text)+"%' or FARMER_CROP_NAME like '%"+str(search_text)+"%' or PROPOSAL_ID like '%"+str(search_text)+"%' or FARMER_AREA_INSURED like '%"+str(search_text)+"%' or PROPOSAL_STATUS like '%"+str(search_text)+"%')"

        print(query)

        data = cx.read_sql(read_connection_string, query,
                           partition_on="PROPOSAL_ID", partition_num=10)
        print("data size: ", data.shape[0])
        data = data.sort_values('PROPOSAL_ID')
        data_size = 100000
        count = 1
        for i in range(0, data.shape[0], data_size):
            print("starting data part ", count)
            b = io.BytesIO()
            writer = pd.ExcelWriter(b, engine='xlsxwriter')
            temp_data = data[i:i+data_size]
            temp_data.to_excel(excel_writer=writer, index=False,
                               sheet_name='SCHEME_UW_REPORT')
            report_param = {
                'STATE': [state],
                'PRODUCT': [product],
                'SEASON': [season],
                'YEAR': [year],
                'DISTRICT': [district],
                'OTHER PARAMS': [params],
                'REPORT GENERATION TIME': [report_time.strftime("%Y-%m-%d %H:%M:%S")],
            }

            df = pd.DataFrame(report_param, columns=[
                'STATE', 'PRODUCT', 'SEASON', 'YEAR', 'DISTRICT', 'OTHER PARAMS', 'REPORT TIME'])
            df.to_excel(excel_writer=writer, index=False,
                        sheet_name='SELECTION PARAMETERS')
            # writer.save()
            writer.close()
            b.seek(0)
            s3.put_object(Body=b, Bucket='sdg-01', Key=s3_report_path + str(reporting_id) +
                          "/SCHEME_UW_Report_" + str(user_id) + "_" + str(reporting_id) + " (" + str(count) + ").xlsx")
            count = count+1

        with conn.cursor() as cur:
            query = "update PMFBY.REPORTING_LOGS set REPORT_AVAILABLE = 1, PROCESS_END_TIME=now() where REPORTING_ID = " + \
                str(reporting_id)+";"
            print("Query: ", query)
            cur.execute(query)
            conn.commit()

        pool.release(conn)
        gc.collect()

    except Exception as e:
        print("Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        e_str = str(e).replace('"', '\\"')
        e_str = e_str.replace("'", "\\'")
        with conn.cursor() as cur:
            query = "update PMFBY.REPORTING_LOGS set REPORT_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = '" + \
                str(e_str)+"' where REPORTING_ID = "+str(reporting_id)+";"
            print("Query: ", query)
            cur.execute(query)
            conn.commit()
        pool.release(conn)
        gc.collect()
        return

def bsb_uw_report_thread(user_id, configid, reporting_id, season, product, year, state, params):
    """
    This method creates a report based on the "params" (which is a JSON String) and stores the same in AWS S3
    """
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

        # *****database entry for process start*****
        report_time = datetime.now()
        with conn.cursor() as cur:
            query = "update PMFBY.REPORTING_LOGS set REPORT_AVAILABLE=0, PROCESS_START_TIME='" + report_time.strftime("%Y-%m-%d %H:%M:%S") + "' where REPORTING_ID=" + \
                str(reporting_id)
            print(reporting_id, query)
            cur.execute(query)
            conn.commit()

        param_dict = json.loads(params)
        district_code = param_dict['district_code']
        district = param_dict['district']
        proposal_status = param_dict['proposal_status']
        crop_name = param_dict['crop_name']
        crop_list = ""

        # columns = ['PROPOSAL_ID', 'PRODUCT_NAME', 'YEAR', 'SEASON', 'STATE', 'FARMER_APPLICATION_ID', 'FARMER_ID', 'FARMER_NAME', 'FARMER_ID_TYPE', 'FARMER_ID_NO', 'FARMER_RELATIVE_NAME', 'FARMER_RELATIVE_RELATION', 'FARMER_MOBILE,FARMER_AGE', 'FARMER_GENDER',
        #            'FARMER_CASTE', 'FARMER_ADDRESS', 'FARMER_L2_NAME', 'FARMER_L2_CODE', 'FARMER_L3_NAME', 'FARMER_L3_CODE', 'FARMER_L4_NAME', 'FARMER_L4_CODE', 'FARMER_L7_NAME', 'FARMER_L7_CODE', 'FARMER_PINCODE', 'BANK_NAME', 'BANK_ID ', 'BANK_BRANCH_NAME ', 'BANK_BRANCH_ID',
        #            'IFSC ', 'FARMER_TYPE ', 'ACCOUNT_OWNERSHIP_TYPE', 'FARMER_BANK_ACCOUNT_NO', 'FARMER_CATEGORY', 'FARMER_NATURE', 'FARMER_ACCOUNT_TYPE', 'INSURANCE_COMPANY_NAME,INSURANCE_COMPANY_CODE', 'FARMER_CROP_NAME', 'CROP_CODE', 'CROP_L2_NAME', 'CROP_L2_CODE', 'CROP_L3_NAME ',
        #            'CROP_L3_CODE', 'INSURANCE_UNIT_LEVEL', 'INSURANCE_UNIT_NAME', 'INSURANCE_UNIT_CODE', 'CROP_L7_NAME', 'CROP_L7_CODE', 'LAND_SURVEY_NUMBER', 'LAND_SUB_DIVISION_NUMBER', 'FARMER_AREA_INSURED', 'FARMER_SHAREOF_PREMIUM', 'STATE_SHAREOF_PREMIUM', 'GOI_SHAREOF_PREMIUM',
        #            'GROSS_PREMIUM', 'SUM_INSURED', 'MIX_CROP', 'COMBINED_INSURED_AREA', 'PREMIUM_DEBIT_DATE', 'SOWING_DATE', 'CUT_OFF_DATE', 'INDEMNITY', 'APPLICATION_STATUS,APPLICATION_SOURCE', 'UTR_TRANSACTION_NUMBER', 'UTR_TRANSACTION_AMOUNT,UTR_TRANSACTION_DATE', 'UTR_TRANSACTION_MODE',
        #            'CROP_L4_NAME', 'CROP_L4_CODE', 'CROP_L5_NAME', 'CROP_L5_CODE', 'CROP_L6_NAME', 'CROP_L6_CODE', 'TIMESTAMP_DATA_GENERATION', 'CREATED_BANK_NAME', 'CREATED_BANK_ID', 'CREATED_BANK_BRANCH_NAME,CREATED_BANK_BRANCH_ID', 'CREATED_USER_NAME', 'CSC_ID', 'APPLICATION_CREATED_DATE',
        #            'LAND_RECORD_FARMER_NAME', 'LAND_RECORD_AREA', 'PROPOSAL_STATUS', 'REMARKS', 'UPLOAD_TASK_ID', 'UPDATED_DATE', 'UPDATE_BY', 'NOTIFICATION_ID', 'ACCOUNT_TAG', 'PARTY_ID', 'CONFIGID']

        query = '''select  PROPOSAL_ID, PRODUCT_NAME, YEAR, SEASON, STATE, FARMER_APPLICATION_ID, FARMER_NAME, FARMER_FATHER_NAME, EPIC_NO, AADHAR_NO, KB_NO, FARMER_AGE, FARMER_GENDER, FARMER_CASTE, FARMER_MOBILE, FARMER_CROP_NAME, CROP_L3_NAME, CROP_L4_NAME, CROP_L5_NAME, CROP_L6_NAME, JL_NO, KHATIAN_NO, PLOT_NO, INSURANCE_UNIT_LEVEL, FARMER_AREA_INSURED, SUM_INSURED, GROSS_PREMIUM, FARMER_CATEGORY, FARMER_NATURE, FARMER_NAME_AS_PER_BANK, BANK_NAME, BANK_BRANCH_NAME, BANK_BRANCH_ID, IFSC, FARMER_BANK_ACCOUNT_NO, APPLICATION_TYPE, APPLICATION_STATUS, APPLICATION_SOURCE, CREATED_USER_NAME, CREATED_USER_TYPE, PROPOSAL_STATUS, REMARKS, UPLOAD_TASK_ID, UPDATED_DATE, UPDATE_BY, NOTIFICATION_ID, ACCOUNT_TAG, PARTY_ID, CONFIGID 
        from PMFBY.BSB_PROPOSAL where configid=''' + str(configid) + '''
        and CROP_L3_NAME=''' + str(district)

        if proposal_status != '%':
            query += ' and PROPOSAL_STATUS="' + str(proposal_status) + '"'

        if crop_name != 'all':
            crop_list = "("
            for x in crop_name:
                crop_list += '"' + str(x) + '",'
            crop_list = crop_list[:-1] + ")"
            print("----crop_list----", crop_list)
            query += ' and FARMER_CROP_NAME in ' + str(crop_list)

        # if search_text:

        #     query += " and (CROP_L3_NAME like '%"+str(search_text)+"%' or CROP_L4_NAME like '%"+str(search_text)+"%' or CROP_L5_NAME like '%"+str(search_text)+"%' or CROP_L6_NAME like '%"+str(search_text)+"%' or CROP_L7_NAME like '%"+str(
        #         search_text)+"%' or SUM_INSURED like '%"+str(search_text)+"%' or GROSS_PREMIUM like '%"+str(search_text)+"%' or FARMER_CROP_NAME like '%"+str(search_text)+"%' or PROPOSAL_ID like '%"+str(search_text)+"%' or FARMER_AREA_INSURED like '%"+str(search_text)+"%' or PROPOSAL_STATUS like '%"+str(search_text)+"%')"

        print(query)

        data = cx.read_sql(read_connection_string, query,
                           partition_on="PROPOSAL_ID", partition_num=10)
        print("data size: ", data.shape[0])
        data = data.sort_values('PROPOSAL_ID')
        data_size = 100000
        count = 1
        for i in range(0, data.shape[0], data_size):
            print("starting data part ", count)
            b = io.BytesIO()
            writer = pd.ExcelWriter(b, engine='xlsxwriter')
            temp_data = data[i:i+data_size]
            temp_data.to_excel(excel_writer=writer, index=False,
                               sheet_name='SCHEME_UW_REPORT')
            report_param = {
                'STATE': [state],
                'PRODUCT': [product],
                'SEASON': [season],
                'YEAR': [year],
                'DISTRICT': [district],
                'OTHER PARAMS': [params],
                'REPORT GENERATION TIME': [report_time.strftime("%Y-%m-%d %H:%M:%S")],
            }

            df = pd.DataFrame(report_param, columns=[
                'STATE', 'PRODUCT', 'SEASON', 'YEAR', 'DISTRICT', 'OTHER PARAMS', 'REPORT TIME'])
            df.to_excel(excel_writer=writer, index=False,
                        sheet_name='SELECTION PARAMETERS')
            # writer.save()
            writer.close()
            b.seek(0)
            s3.put_object(Body=b, Bucket='sdg-01', Key=s3_report_path + str(reporting_id) +
                          "/SCHEME_UW_Report_" + str(user_id) + "_" + str(reporting_id) + " (" + str(count) + ").xlsx")
            count = count+1

        with conn.cursor() as cur:
            query = "update PMFBY.REPORTING_LOGS set REPORT_AVAILABLE = 1, PROCESS_END_TIME=now() where REPORTING_ID = " + \
                str(reporting_id)+";"
            print("Query: ", query)
            cur.execute(query)
            conn.commit()

        pool.release(conn)
        gc.collect()

    except Exception as e:
        print("Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        e_str = str(e).replace('"', '\\"')
        e_str = e_str.replace("'", "\\'")
        with conn.cursor() as cur:
            query = "update PMFBY.REPORTING_LOGS set REPORT_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = '" + \
                str(e_str)+"' where REPORTING_ID = "+str(reporting_id)+";"
            print("Query: ", query)
            cur.execute(query)
            conn.commit()
        pool.release(conn)
        gc.collect()
        return


@pmfby.route('/pmfby_report_log', methods=['POST'])
def pmfby_report_log():
    print("---------pmfby_report_log-----------")
    global pool
    try:
        reporting_id = -1
        print(pool)
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
        print(conn)
        print(request.form)
        user_id = request.form.get('userid')
        configid = request.form.get('configid')
        if configid is None:
            configid = 'NULL'
        # configid = int(configid)
        method = request.form.get('method')
        params = request.form.get('params')

        params = params.replace('"', '\\"')
        params = params.replace("'", "\\'")

        with conn.cursor() as cur:
            query = "SELECT * from PMFBY.REPORTING_LOGS where REQUEST_BY=" + str(user_id) + " and CONFIGID=" + str(
                configid) + " and METHOD='" + str(method) + "' and PARAMS='" + str(params) + "' and REPORT_AVAILABLE in (0, -1)"
            print("Query: ", query)
            cur.execute(query)
            rc = cur.rowcount
            if rc > 0:
                print("Calm down")
                message = {
                    'status': 200,
                    'message':  "Report already under process"
                }
                response = jsonify(message)
                response.status_code = 200
                return response

        # Insert starting Entry into file upload table
        with conn.cursor() as cur:
            query = "INSERT INTO PMFBY.REPORTING_LOGS(METHOD,REQUEST_BY,CONFIGID,REQUEST_TIME,PARAMS,REPORT_AVAILABLE) values('" + str(method) + "','"+str(user_id)+"',"+str(configid) + \
                ",now(),'" + str(params) + "',-1);"
            print("Query: ", query)
            cur.execute(query)
            conn.commit()
            reporting_id = cur.lastrowid
        print("report_log", reporting_id)

        message = {
            'status': 200,
            'message':  "1"
        }
        response = jsonify(message)
        response.status_code = 200
        return response

    except Exception as e:
        print("Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        e_str = str(e).replace('"', '\\"')
        e_str = e_str.replace("'", "\\'")
        if reporting_id != -1:
            with conn.cursor() as cur:
                query = "update PMFBY.REPORTING_LOGS set REPORT_AVAILABLE = 2, REMARK = '" + \
                    str(e_str)+"' where id = "+str(reporting_id)+";"
                # print("Query: ", query)
                cur.execute(query)
                conn.commit()
        message = {
            'status': 502,
            'message':  str(e)
        }
        respone = jsonify(message)
        respone.status_code = 502
        pool.release(conn)
        gc.collect()
        return respone


def pending_report(request_by: int, configid: int, reporting_id: int, season: str, product: str, year: int, state: str, method: str, params: str):
    """
    Creates an instance of "Reporting_thread" class and starts the thread

    Parameter "params" should be JSON string
    """

    try:

        new_task = Reporting_thread(request_by, configid, reporting_id,
                                    season, product, year, state, method, params)
        new_task.start()
        new_task.join(7200)
        new_task.raise_exception()
        new_task.join()

        return
    except Exception as e:
        print("Exception Occurred for reporting id ", reporting_id)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        return


@pmfby.route('/check_report_log', methods=['GET'])
def check_report_log():

    try:
        try:
            conn = pymysql.connect(host=endpoint, user=username, password=password, database=database_name, ssl={
                                   "true": True}, connect_timeout=5, cursorclass=pymysql.cursors.DictCursor, autocommit=True)
        except pymysql.MySQLError as e:
            print(
                "SCHEDULER ERROR: Unexpected error: Could not connect to MySQL instance.")
            print(e)
            # return
            gc.collect()
            message = {
                'status': 502,
                'message': "ERROR: Unexpected error: Could not connect to MySQL instance.",
            }
            respone = jsonify(message)
            respone.status_code = 502
            return respone

        reporting_id = -1
        with conn.cursor() as cur:
            query = "select * from PMFBY.REPORTING_LOGS where REPORT_AVAILABLE=0"
            # print(query)
            cur.execute(query)
            rc = cur.rowcount
            result = cur.fetchall()
            # print(result)
            # print(type(result))

        if rc > 0:
            print("REPORTING: Total ongoing task: ", rc)
            killed_process = []
            ongoing_process = []
            for x in result:
                reporting_id = x['REPORTING_ID']
                # print("time diff of " + str(upload_task_id) + ": ", (datetime.now() - x['PROCESS_START_TIME']).total_seconds())

                if (datetime.now() - x['PROCESS_START_TIME']).total_seconds() > 7200:
                    killed_process.append(reporting_id)
                    query = "update PMFBY.REPORTING_LOGS set REPORT_AVAILABLE=2, PROCESS_END_TIME=now() where REPORTING_ID = " + \
                        str(x['REPORTING_ID'])
                    # print(query)
                    with conn.cursor() as cur:
                        cur.execute(query)
                        conn.commit()
                else:
                    ongoing_process.append(reporting_id)

                conn.close()
                gc.collect()
                # return
                msg = "ONGOING PROCESS: " + \
                    ','.join([str(i) for i in ongoing_process])
                if len(killed_process) > 0:
                    msg += " | KILLED PROCESS: " + \
                        ','.join([str(i) for i in killed_process])
                message = {
                    'status': 200,
                    'message':  msg,
                    'task_id': -1
                }
                print("REPORTING Response: ", message['message'])
                response = jsonify(message)
                response.status_code = 200
                return response

        else:
            # change product name
            query = """select a.REQUEST_BY, a.CONFIGID, a.REPORTING_ID, a.METHOD, a.PARAMS, b.SEASON, b.PRODUCT, b.YEAR, b.STATE
            from PMFBY.REPORTING_LOGS a
            left join PMFBY.PRODUCT_CONFIG b on a.CONFIGID=b.CONFIGID
            where REPORT_AVAILABLE=-1
            order by REPORTING_ID
            limit 1"""
            # print(query)
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchone()
            # print(result)
            if result != None:
                reporting_id = result['REPORTING_ID']
                print("REPORTING Process Started:  ", reporting_id)
                # change product name
                thread = Thread(target=pending_report, kwargs={'request_by': result['REQUEST_BY'], 'configid': result['CONFIGID'], 'reporting_id': result['REPORTING_ID'], 'season': result[
                                'SEASON'], 'product': result['PRODUCT'], 'year': result['YEAR'], 'state': result['STATE'], 'method': result['METHOD'], 'params': result['PARAMS']})
                thread.start()

                # if result['METHOD'] == "PMFBY_Underwriting_Creation":
                # 	pmfby.uw_upload_thread(result['UPLOADED_BY'], result['CONFIGID'], result['ID'], result['SEASON'], result['PRODUCT'], result['YEAR'], result['STATE'])
                # elif result['METHOD'] == "PMFBY_Notification_Creation":
                # 	pmfby.notifiction_upload_thread(result['UPLOADED_BY'], result['CONFIGID'], result['ID'])

                # pool.release(conn)
                conn.close()
                gc.collect()
                # return
                message = {
                    'status': 200,
                    'message':  "PROCESS STARTED",
                    'task_id': reporting_id
                }
                response = jsonify(message)
                response.status_code = 200
                return response

            else:
                print("REPORTING No process pending")
                conn.close()
                gc.collect()
                # return
                message = {
                    'status': 200,
                    'message':  "NO PROCESS PENDING",
                    'task_id': -1
                }
                response = jsonify(message)
                response.status_code = 200
                return response

    except Exception as e:
        print("REPORTING Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        print("REPORTING Exception: ", e)
        conn.close()
        gc.collect()
        message = {
            'status': 502,
            'message': str(e),
            'task_id': reporting_id
        }
        respone = jsonify(message)
        respone.status_code = 502
        return respone


@pmfby.route('/download_report_files', methods=['GET'])
def download_report_files():
    global pool
    print(pool)
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
    print(conn)
    try:
        id = request.args.get('id')
        print("downloading files for reporting_id ", id)
        s = io.BytesIO()
        filename = 'report_' + str(id)+'.zip'
        zf = zipfile.ZipFile(s, 'w')
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket='sdg-01', Prefix=s3_report_path + str(id) + '/')
        for page in page_iterator:
            if page['KeyCount'] > 0:
                # print(upload_log_id, page)
                for file in page['Contents']:
                    # print(upload_log_id, file)
                    key = file['Key']
                    if '.' not in key:
                        continue
                    data = s3.get_object(Bucket='sdg-01', Key=key)
                    zf.writestr(str(key[key.rindex("/")+1:]),
                                data.get('Body').read())

        zf.close()
        s.seek(0)
        gc.collect()
        print("done")
        respone = flask.make_response(flask.send_file(
            s, mimetype='application/zip', as_attachment=True, download_name=filename))
        respone.status_code = 200
        return respone

    except Exception as e:
        print("Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        conn.close()
        gc.collect()
        message = {
            'status': 502,
            'message': str(e)
        }
        respone = jsonify(message)
        respone.status_code = 502
        return respone


def notifiction_upload_thread_bsb(user_id: int, configid: int, upload_log_id: int, season: str, year: int, state: str, state_code: int):
    """
    For BSB Product
    Reads Notification file from AWS S3 based on "upload_log_id" and insert the same in Notification table
    """
    global bsb_notif_row_kharif, bsb_notif_row_rabi, pool
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

        # *****************Column Mapping****************************
        dictionary = {'STATE NAME': 'STATE',	'DISTRICT NAME': 'L3_NAME', 'L&LR DCODE': 'L3_NAME_CODE', 'BLOCK NAME': 'L4_NAME', 'L&LR BCODE': 'L4_NAME_CODE',
                      'PANCHAYAT NAME': 'L5_NAME', 'ING GPCODE': 'L5_NAME_CODE', 'CROP': 'CROP_NAME', 'NOTIFICATION UNIT': 'NOTIFICATION_UNIT', 'NOTIFICATION STATUS': 'NOTIFICATION_STATUS',
                      'INSURANCE COMPANY': 'INSURANCE_COMPANY_NAME',	'SCALE OF FINANCE': 'SUM_INSURED', 'PREMIUM RATE': 'PREMIUM_RATE_PCT', 'PREMIUM RATE (%)': 'PREMIUM_RATE_PCT',
                      'STATE SHARE\n%': 'STATE_SHARE_PCT', 'GOVT. \nSHARE %': 'STATE_SHARE_PCT', 'FARMER \nSHARE %': 'FARMER_SHARE_PCT', 'FARMER SHARE \n%': 'FARMER_SHARE_PCT', 'INDEMNITY %': 'INDEMNITY'}

        time_now = datetime.now()
        time_get = time_now.strftime("%Y%m%d%H%M%S")
        updated_date_time = time_now.strftime("%Y-%m-%d %H:%M:%S")
        file_name = 'bsb_noti_'+str(user_id)+"_" + \
            str(configid)+"_"+str(time_get)+".csv"

        if season == 'RABI':
            columns = bsb_notif_row_rabi
        else:
            columns = bsb_notif_row_kharif

        excel_data1 = upload_thread_DF_init(conn, upload_log_id, columns, exclude_columns=[
                                            'FILENAME'], header_rename=dictionary)
        if type(excel_data1) != pd.DataFrame:
            pool.release(conn)
            gc.collect()
            return
        excel_data1 = excel_data1.drop(['CLUSTER', 'PREMIUM/HA (RS.)', 'FARMER SHARE/HA (RS.)',
                                        'STATE SHARE/HA (RS.)'], errors='ignore', axis=1)

        print(upload_log_id, excel_data1.shape[0])
        no_of_records_uploaded = excel_data1.shape[0]

        # ********Fetching data from BSB_Notification Table****************
        query = "SELECT NOTIFICATION_ID,CROP_NAME, CONFIGID, L3_NAME_CODE, L4_NAME_CODE, L5_NAME_CODE FROM PMFBY.BSB_NOTIFICATION WHERE CONFIGID=" + \
            str(configid)
        print("query--------", query)
        # exist_data = pd.read_sql(query,
        #                         read_connection_string, dtype={'CONFIGID': str, 'L3_NAME_CODE': str, 'L4_NAME_CODE': str, 'L5_NAME_CODE': str})
        exist_data = pd.read_sql(query,
                                 read_connection_string)

        exist_data = exist_data.apply(
            lambda x: x.astype(str).str.upper().str.strip())

        excel_data1['CONFIGID'] = configid

        excel_data1['UPLOAD_TASK_ID'] = upload_log_id

        excel_data1['UPDATED_USERID'] = user_id

        excel_data1['UPDATED_DATE'] = updated_date_time

        excel_data1 = excel_data1.apply(
            lambda x: x.astype(str).str.upper().str.strip())

        excel_data1 = excel_data1.replace('', None)
        print("configid----upload_log_id----user_id----updated_date_time----",
              configid, upload_log_id, user_id, updated_date_time)
        duplicate_list = ['CROP_NAME', 'CONFIGID', 'L3_NAME_CODE',
                          'L4_NAME_CODE', 'L5_NAME_CODE']
        de_duplicate_list = ['PREMIUM_RATE_PCT', 'FARMER_SHARE_PCT', 'STATE_SHARE_PCT',
                             'INDEMNITY', 'SUM_INSURED', 'NOTIFICATION_UNIT']

        dfObject = CheckValidity(excel_data1)

        # ************DUPLICATE NOTIFICATION CHECK IN DATABASE*************
        exec_status = dfObject.removeDuplicateFromDf(
            exist_data, duplicate_list, "DUPLICATE NOTIFICATION EXISTS IN DATABASE")
        if exec_status != True:
            raise Exception(exec_status)

        # ************DUPLICATE NOTIFICATION CHECK IN FILE*************
        exec_status = dfObject.removeDuplicateSelf(
            duplicate_list, "DUPLICATE NOTIFICATION IN FILE", True, de_duplicate_list)
        if exec_status != True:
            raise Exception(exec_status)

        valuesToBeChecked = {
            'CROP_NAME': None,
            'STATE': [state],
            'L3_NAME': None,
            'L4_NAME': None,
            'L5_NAME': None,
            'PREMIUM_RATE_PCT': None,
            'FARMER_SHARE_PCT': None,
            'STATE_SHARE_PCT': None,
            'INDEMNITY': None,
            'SUM_INSURED': None,
            'NOTIFICATION_UNIT': None,
            'NOTIFICATION_STATUS': ['NOTIFIED'],
        }

        numericColumns = {
            'PREMIUM_RATE_PCT': 'isnumeric',
            'FARMER_SHARE_PCT': 'isnumeric',
            'STATE_SHARE_PCT': 'isnumeric',
            'INDEMNITY': 'isnumeric',
            'SUM_INSURED': 'isnumeric',
            'L3_NAME_CODE': 'isinteger',
            'L4_NAME_CODE': 'isinteger',
            'L5_NAME_CODE': 'isinteger',
        }
        # *****Data validation for necessary columns and datatypes*****
        exec_status = dfObject.checkValue(valuesToBeChecked)
        if exec_status != True:
            raise Exception(exec_status)

        exec_status = dfObject.checkValue(numericColumns)
        if exec_status != True:
            raise Exception(exec_status)

        data_csv = dfObject.getValidDf().drop(
            ['REMARK', 'BUSINESS_LOGIC_VALID', 'FILENAME'], axis=1)

        # Insert feeback in to S3
        buf = io.BytesIO()
        feedback_df = dfObject.getInvalidDf().drop(['CONFIGID', 'UPLOAD_TASK_ID', 'UPDATED_USERID',
                                                    'UPDATED_DATE', 'BUSINESS_LOGIC_VALID'], axis=1)
        feedback_df.to_csv(buf, encoding='utf-8', index=False)

        buf.seek(0)
        s3.put_object(Body=buf, Bucket='sdg-01',
                      Key=s3_feedback_path + str(upload_log_id) + "/Feedback.csv")

        # *************Inserting data into BSB_NOTIFICATION****************
        if data_csv.shape[0] != 0:
            data_csv['NOTIFICATION_STATUS'] = 1
            data_csv.insert(0, "NOTIFICATION_ID", None)
            load_file_path = file_path + "/" + file_name
            # ***** upload csv file to DB *****
            up_status = upload_file_to_DB(
                conn, data_csv, load_file_path, "PMFBY.BSB_NOTIFICATION")
            if up_status != 1:
                raise Exception(up_status)

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

# def pv_collection_upload_thread(user_id, upload_log_id):

#     try:
#         print("1", upload_log_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
#         time_now = datetime.now()
#         global pv_coll_row, pool
#         print(user_id,  upload_log_id)

#         # *****Creating connection*****
#         try:
#             conn = pool.get_conn()
#             with conn.cursor() as cur:
#                 query = "select now()"
#                 cur.execute(query)
#                 n = cur.fetchone()
#         except:
#             print(upload_log_id, 'Trying to reconnect')
#             pool.init()
#             conn = pool.get_conn()

#         # *****Database entry for process starts*****

#         with conn.cursor() as cur:
#             query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE=0, PROCESS_START_TIME=now() where ID=" + \
#                 str(upload_log_id)
#             print(upload_log_id, query)
#             cur.execute(query)
#             conn.commit()

#         with conn.cursor() as cur:
#             query = "select FILETYPE from PMFBY.FILEUPLOAD_LOGS where ID=" + \
#                 str(upload_log_id)
#             print(upload_log_id, query)
#             cur.execute(query)
#             result = cur.fetchone()
#         filetype = result['FILETYPE']
#         if filetype not in ['excel', 'csv']:
#             with conn.cursor() as cur:
#                 query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Invalid file type' where id = " + \
#                     str(upload_log_id)+";"
#                 print("Query: ", query)
#                 cur.execute(query)
#                 conn.commit()
#             pool.release(conn)
#             gc.collect()
#             return
#         time_now = datetime.now()
#         time_get = time_now.strftime("%Y%m%d%H%M%S")
#         updated_date_time = time_now.strftime("%Y-%m-%d %H:%M:%S")
#         file_name = 'pv_coll'+str(user_id)+"_" + "_"+str(time_get)+".csv"

#         pv_collection_data = read_files_from_s3(upload_log_id, filetype)
#         if type(pv_collection_data) is dict:
#             with conn.cursor() as cur:
#                 e_str = str(pv_collection_data["error"]).replace('"', '\\"')
#                 e_str = e_str.replace("'", "\\'")
#                 query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Error while reading file', EXCEPTION = '" + e_str + "' where id = " + \
#                     str(pv_collection_data)+";"
#                 print("Query: ", query)
#                 cur.execute(query)
#                 conn.commit()
#             pool.release(conn)
#             gc.collect()
#             return

#         elif pv_collection_data.shape[0] == 0:
#             with conn.cursor() as cur:
#                 query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Upload file(s) is/are empty' where id = " + \
#                     str(upload_log_id)+";"
#                 print("Query: ", query)
#                 cur.execute(query)
#                 conn.commit()
#             pool.release(conn)
#             gc.collect()
#             return

#         print(upload_log_id, pv_collection_data.shape[0])

#         ######### Check if headers don't match #########
#         columns = pv_coll_row
#         headers = list(pv_collection_data.columns.values)
#         headers.remove('FILENAME')
#         # print("headerssssss s3file",headers)
#         # print("columnsssssss",columns)

#         try:
#             if(headers != columns):
#                 with conn.cursor() as cur:
#                     query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Headers do not match' where id = " + \
#                         str(upload_log_id)+";"
#                     print("Query: ", query)
#                     cur.execute(query)
#                     conn.commit()
#                 pool.release(conn)
#                 gc.collect()
#                 return
#         except Exception as e:
#             print("Exception Occurred")
#             exc_type, exc_obj, exc_tb = sys.exc_info()
#             fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#             print(exc_type, fname, exc_tb.tb_lineno)
#             print("Exception: ", e)
#             e_str = str(e).replace('"', '\\"')
#             e_str = e_str.replace("'", "\\'")
#             with conn.cursor() as cur:
#                 query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Something went wrong', EXCEPTION = '" + \
#                     str(e_str)+"' where id = "+str(upload_log_id)+";"
#                 print("Query: ", query)
#                 cur.execute(query)
#                 conn.commit()
#             pool.release(conn)
#             gc.collect()
#             return

#         dictionary = {'SR NO': 'SR_NO', 'DEBIT TYPE*': 'DEBIT_TYPE', 'PAY TYPE*': 'PAY_TYPE', 'GROSS AMOUNT*': 'GROSS_AMOUNT',
#                       'REFUND*': 'REFUND', 'CHARGEBACK': 'CHARGEBACK', 'LATE RETURN': 'LATE_RETURN', 'CHARGES': 'CHARGES',
#                       'SERVICETAX': 'SERVICETAX', 'SURCHARGE': 'SURCHARGE', 'TDS': 'TDS', 'NET AMOUNT*': 'NET_AMOUNT',
#                       'PV NO*': 'PV_NO', 'FILE DATE*': 'FILE_DATE', 'MERCHANT NAME*': 'MERCHANT_NAME',
#                       'PAYMENT REPORT VOUCHER*': 'PAYMENT_REPORT_VOUCHER', 'ICNAME': 'ICNAME', 'ID': 'ID',
#                       'SEASON': 'SEASON', 'YEAR': 'YEAR', 'REMARKS': 'REMARKS'}

#         pv_collection_data.rename(columns=dictionary, inplace=True)

#         no_of_records_uploaded = pv_collection_data.shape[0]

#         pv_collection_data['UPLOAD_TASK_ID'] = upload_log_id

#         pv_collection_data['UPDATED_USERID'] = user_id

#         pv_collection_data['UPDATED_DATE'] = updated_date_time

#         pv_collection_data = pv_collection_data.apply(
#             lambda x: x.astype(str).str.upper().str.strip())

#         pv_collection_data = pv_collection_data.replace(
#             '\\\\', '\\\\\\\\', regex=True)

#         # pv_collection_data[['CROP_CODE', 'STATE_CODE', 'L3_NAME_CODE', 'L4_NAME_CODE', 'L5_NAME_CODE', 'L6_NAME_CODE', 'L7_NAME_CODE']] = pv_collection_data[['CROP_CODE', 'STATE_CODE', 'L3_NAME_CODE', 'L4_NAME_CODE', 'L5_NAME_CODE', 'L6_NAME_CODE', 'L7_NAME_CODE']].apply(
#         #     lambda x: x.str.lstrip('0'))

#         pv_collection_data = pv_collection_data.replace('', None)

#         duplicate_list = ['PV_NO']
#         # de_duplicate_list = ['PREMIUM_RATE_PCT', 'FARMER_SHARE_PCT', 'GOI_SHARE_PCT', 'STATE_SHARE_PCT',
#         #                      'INDEMNITY', 'SUM_INSURED', 'CUT_OFF_DATE', 'NOTIFIED_IU_NAME', 'INSURANCE_UNIT_NAME']

#         valuesToBeChecked = {
#             'DEBIT_TYPE': None,
#             'PAY_TYPE': None,
#             'PV_NO': None,
#             'FILE_DATE': None,
#             'MERCHANT_NAME': None,
#             'PAYMENT_REPORT_VOUCHER': None,
#             'ICNAME': None,
#             'GROSS_AMOUNT': None,
#             'REFUND': None,
#             'CHARGEBACK': None,
#             'LATE_RETURN': None,
#             'CHARGES': None,
#             'SERVICETAX': None,
#             'SURCHARGE': None,
#             'TDS': None,
#             'NET_AMOUNT': None,

#         }

#         numericColumns = {
#             'GROSS_AMOUNT': 'isnumeric',
#             'REFUND': 'isnumeric',
#             'CHARGEBACK': 'isnumeric',
#             'LATE_RETURN': 'isnumeric',
#             'CHARGES': 'isnumeric',
#             'SERVICETAX': 'isnumeric',
#             'SURCHARGE': 'isnumeric',
#             'TDS': 'isnumeric',
#         }

#         dfObject = CheckValidity(pv_collection_data)

#         exec_status = dfObject.removeDuplicateSelf(
#             duplicate_list, "Duplicate PV No. Found", True)
#         if exec_status != True:
#             raise Exception(exec_status)

#         exec_status = dfObject.checkValue(valuesToBeChecked)
#         if exec_status != True:
#             raise Exception(exec_status)

#         exec_status = dfObject.checkValue(numericColumns)
#         if exec_status != True:
#             raise Exception(exec_status)

#         data_csv = dfObject.getValidDf().drop(
#             ['REMARK', 'BUSINESS_LOGIC_VALID'], errors='ignore', axis=1)

#         # Insert feeback in to S3
#         buf = io.BytesIO()
#         feedback_df = dfObject.getInvalidDf().drop(
#             ['UPLOAD_TASK_ID', 'UPDATED_USERID', 'UPDATED_DATE',  'BUSINESS_LOGIC_VALID'], errors='ignore', axis=1)
#         feedback_df.to_csv(buf, encoding='utf-8', index=False)

#         buf.seek(0)
#         s3.put_object(Body=buf, Bucket='sdg-01',
#                       Key=s3_feedback_path + str(upload_log_id) + "/Feedback.csv")

#         load_file_path = file_path + "/" + file_name

#         data_csv.to_csv(load_file_path, encoding='utf-8', header=True,
#                         doublequote=True, sep=',', index=False, na_rep='\\N', quotechar='"')
#         if data_csv.shape[0] != 0:
#             with conn.cursor() as cur:
#                 load_sql = "LOAD DATA LOCAL INFILE '" + load_file_path + \
#                     "' INTO TABLE PMFBY.COLLECTION_PV FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES;"
#                 print(load_sql)
#                 cur.execute(load_sql)
#                 conn.commit()

#         os.remove(load_file_path)

#         no_of_records_success = data_csv.shape[0]
#         no_of_records_failed = (no_of_records_uploaded-no_of_records_success)

#         # Update Feedback Available
#         with conn.cursor() as cur:
#             query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 1, PROCESS_END_TIME=now(), FEEDBACK_FILE = 'Feedback.csv', SUCCESS = "+str(no_of_records_success)+", FAILED = " + \
#                 str(no_of_records_failed) + \
#                 "  where id = "+str(upload_log_id)+";"
#             print("Query: ", query)
#             cur.execute(query)
#             conn.commit()

#         pool.release(conn)
#         gc.collect()
#         print("2", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
#         return

#     except Exception as e:
#         print("Exception Occurred")
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#         print(exc_type, fname, exc_tb.tb_lineno)
#         print("Exception: ", e)
#         e_str = str(e).replace('"', '\\"')
#         e_str = e_str.replace("'", "\\'")
#         with conn.cursor() as cur:
#             query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Something went wrong', EXCEPTION = '" + \
#                 str(e_str)+"' where id = "+str(upload_log_id)+";"
#             # print("Query: ", query)
#             cur.execute(query)
#             conn.commit()

#         gc.collect()
#         return


# def tid_collection_upload_thread(user_id, configid, upload_log_id, season, year, state, state_code):
#     global tid_coll_row, pool
#     try:
#         print("1", upload_log_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
#         print(pool)
#         try:
#             conn = pool.get_conn()
#             with conn.cursor() as cur:
#                 query = "select now()"
#                 cur.execute(query)
#                 n = cur.fetchone()
#         except:
#             print(upload_log_id, 'Trying to reconnect')
#             pool.init()
#             conn = pool.get_conn()
#         print(conn)

#         with conn.cursor() as cur:
#             query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE=0, PROCESS_START_TIME=now() where ID=" + \
#                 str(upload_log_id)
#             print(upload_log_id, query)
#             cur.execute(query)
#             conn.commit()

#         with conn.cursor() as cur:
#             query = "select FILETYPE from PMFBY.FILEUPLOAD_LOGS where ID=" + \
#                 str(upload_log_id)
#             print(upload_log_id, query)
#             cur.execute(query)
#             result = cur.fetchone()
#         filetype = result['FILETYPE']
#         if filetype not in ['excel', 'csv']:
#             with conn.cursor() as cur:
#                 query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Invalid file type' where id = " + \
#                     str(upload_log_id)+";"
#                 print("Query: ", query)
#                 cur.execute(query)
#                 conn.commit()
#             pool.release(conn)
#             gc.collect()
#             return

#         time_now = datetime.now()
#         time_get = time_now.strftime("%Y%m%d%H%M%S")
#         updated_date_time = time_now.strftime("%Y-%m-%d %H:%M:%S")
#         file_name = 'tid_coll'+str(user_id)+"_" + \
#             str(configid)+"_"+str(time_get)+".csv"

#         tid_collection_data = read_files_from_s3(upload_log_id, filetype)
#         if type(tid_collection_data) is dict:
#             with conn.cursor() as cur:
#                 e_str = str(tid_collection_data["error"]).replace('"', '\\"')
#                 e_str = e_str.replace("'", "\\'")
#                 query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK='Something went wrong', EXCEPTION = 'Error while reading file. " + e_str + "' where id = " + \
#                     str(upload_log_id)+";"
#                 print("Query: ", query)
#                 cur.execute(query)
#                 conn.commit()
#             pool.release(conn)
#             gc.collect()
#             return

#         elif tid_collection_data.shape[0] == 0:
#             with conn.cursor() as cur:
#                 query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Upload file(s) is/are empty' where id = " + \
#                     str(upload_log_id)+";"
#                 print("Query: ", query)
#                 cur.execute(query)
#                 conn.commit()
#             pool.release(conn)
#             gc.collect()
#             return

#         print(upload_log_id, tid_collection_data.shape[0])

#         ##### Checking if headers are matching #####

#         columns = tid_coll_row
#         headers = list(tid_collection_data.columns.values)
#         headers.remove('FILENAME')

#         try:
#             if(headers != columns):
#                 with conn.cursor() as cur:
#                     query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Headers do not match' where id = " + \
#                         str(upload_log_id)+";"
#                     print("Query: ", query)
#                     cur.execute(query)
#                     conn.commit()
#                 pool.release(conn)
#                 gc.collect()
#                 return
#         except Exception as e:
#             print("Exception Occurred")
#             exc_type, exc_obj, exc_tb = sys.exc_info()
#             fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#             print(exc_type, fname, exc_tb.tb_lineno)
#             print("Exception: ", e)
#             e_str = str(e).replace('"', '\\"')
#             e_str = e_str.replace("'", "\\'")
#             with conn.cursor() as cur:
#                 query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK='Something went wrong', EXCEPTION = '" + \
#                     str(e_str)+"' where id = "+str(upload_log_id)+";"
#                 print("Query: ", query)
#                 cur.execute(query)
#                 conn.commit()
#             pool.release(conn)
#             gc.collect()
#             return

#         dictionary = {'ID': 'ID', 'BILLERNAME': 'BILLER_NAME', 'DEBITTYPE': 'DEBIT_TYPE', 'NARRATION': 'NARRATION', 'PAYMODE': 'PAY_MODE', 'PRODUCTCODE': 'PRODUCT_CODE', 'BDREFNO': 'BD_REF_NO', 'OURID': 'OUR_ID', 'REF1': 'REF1', 'REF2': 'REF2', 'REF3': 'REF3', 'REF4': 'REF4',
#                       'CREATEDON': 'CREATED_ON', 'GROSSAMOUNT': 'GROSS_AMOUNT', 'CHARGES': 'CHARGES', 'SERVICETAX': 'SERVICE_TAX', 'SURCHARGE': 'SURCHARGE', 'TDS': 'TDS', 'NETAMOUNT': 'NET_AMOUNT', 'FILENAME': 'FILENAME', 'SCHEME': 'SCHEME', 'SEASON': 'SEASON', 'YEAR': 'YEAR',
#                       'STATE': 'STATE', 'SOURCE': 'SOURCE', 'POLICYID': 'POLICY_ID', 'BANKNAME': 'BANK_NAME', 'BRANCHCODE': 'BRANCH_CODE', 'BRANCHID': 'BRANCH_ID', 'BRANCHNAME': 'BRANCH_NAME', 'IFSC': 'IFSC', 'REFUND': 'REFUND', 'DUPLICATE': 'DUPLICATE'}

#         tid_collection_data.rename(columns=dictionary, inplace=True)
#         # tid_collection_data = tid_collection_data.drop()

#         no_of_records_uploaded = tid_collection_data.shape[0]

#         tid_collection_data['CONFIGID'] = configid

#         tid_collection_data['UPLOAD_TASK_ID'] = upload_log_id

#         tid_collection_data['UPDATED_USERID'] = user_id

#         tid_collection_data['UPDATED_DATE'] = updated_date_time

#         tid_collection_data = tid_collection_data.apply(
#             lambda x: x.astype(str).str.upper().str.strip())

#         tid_collection_data = tid_collection_data.replace('', None)

#         duplicate_list = ['BD_REF_NO']
#         # de_duplicate_list = []

#         dfObject = CheckValidity(tid_collection_data)

#         exec_status = dfObject.removeDuplicateFromDf(
#             duplicate_list, "Duplicate BD Ref No. found")
#         if exec_status != True:
#             raise Exception(exec_status)

#         exec_status = dfObject.removeDuplicateSelf(
#             duplicate_list, "Duplicate BD Ref No. found in file", True)
#         if exec_status != True:
#             raise Exception(exec_status)

#         valuesToBeChecked = {
#             'ID': None,
#             'BILLER_NAME': None,
#             'DEBIT_TYPE': None,
#             'NARRATION': None,
#             'PAY_MODE': None,
#             'PRODUCT_CODE': None,
#             'BD_REF_NO': None,
#             'INDEMNITY': None,
#             'OUR_ID': None,
#             'BANK_NAME': None,
#             'BRANCH_CODE': None,
#             'BRANCH_ID': None,
#             'BRANCH_NAME': None,
#             'IFSC': None,
#         }

#         numericColumns = {
#             'GROSS_AMOUNT': 'isnumeric',
#             'CHARGES': 'isnumeric',
#             'SERVICE_TAX': 'isnumeric',
#             'SURCHARGE': 'isnumeric',
#             'TDS': 'isnumeric',
#             'NET_AMOUNT': 'isnumeric',
#             'REFUND': 'isnumeric',
#         }

#         exec_status = dfObject.checkValue(valuesToBeChecked)
#         if exec_status != True:
#             raise Exception(exec_status)

#         exec_status = dfObject.checkValue(numericColumns)
#         if exec_status != True:
#             raise Exception(exec_status)

#         data_csv = dfObject.getValidDf().drop(
#             ['REMARK', 'BUSINESS_LOGIC_VALID'], axis=1)

#         # # ***********Adding remaining columns of PMFBY****************
#         # col = ['PMFBY_NOTIF_ID', 'CROP_NAME', 'CROP_CODE', 'SSSYID', 'STATE_CODE', 'L4_TERM', 'L5_TERM', 'L6_NAME', 'L6_NAME_CODE', 'L6_TERM', 'L7_NAME', 'L7_NAME_CODE', 'INSURANCE_UNIT_NAME',
#         #        'INSURANCE_COMPANY_CODE', 'GOI_SHARE_PCT', 'CUT_OFF_DATE', 'THRESHOLD_YIELD', 'DRIAGE_FACTOR', 'EXPECTED_SUM_INSURED', 'NO_OF_CCE_REQD', 'CCE_AREA', 'EXPECTED_PREMIUM', 'GOVT_THRESHOLD_YIELD', 'GOVT_ACTUAL_YIELD']

#         # data_csv = pd.concat([data_csv, pd.DataFrame(columns=col)])

#         # data_csv = data_csv.reindex(['PMFBY_NOTIF_ID', 'CROP_NAME', 'CROP_CODE', 'SSSYID', 'STATE', 'STATE_CODE', 'L3_NAME_CODE', 'L3_NAME', 'L4_NAME', 'L4_NAME_CODE', 'L4_TERM', 'L5_NAME', 'L5_NAME_CODE', 'L5_TERM', 'L6_NAME', 'L6_NAME_CODE', 'L6_TERM', 'L7_NAME', 'L7_NAME_CODE', 'INSURANCE_UNIT_NAME',
#         #                              'INSURANCE_COMPANY_CODE', 'INSURANCE_COMPANY_NAME', 'PREMIUM_RATE_PCT', 'FARMER_SHARE_PCT', 'GOI_SHARE_PCT', 'STATE_SHARE_PCT', 'INDEMNITY', 'SUM_INSURED', 'CUT_OFF_DATE', 'NOTIFIED_IU_NAME', 'THRESHOLD_YIELD', 'DRIAGE_FACTOR', 'EXPECTED_SUM_INSURED', 'NO_OF_CCE_REQD', 'CCE_AREA', 'EXPECTED_PREMIUM', 'GOVT_THRESHOLD_YIELD', 'GOVT_ACTUAL_YIELD', 'CONFIGID',
#         #                              'UPLOAD_TASK_ID', 'UPDATED_USERID', 'UPDATED_DATE', 'NOTIFICATION_STATUS', 'LAST_DENOTIFY_DATE'], axis=1)

#         # Insert feeback in to S3
#         buf = io.BytesIO()
#         feedback_df = dfObject.getInvalidDf().drop(['CONFIGID', 'UPLOAD_TASK_ID', 'UPDATED_USERID',
#                                                     'UPDATED_DATE', 'BUSINESS_LOGIC_VALID'], axis=1)
#         feedback_df.to_csv(buf, encoding='utf-8', index=False)

#         buf.seek(0)
#         s3.put_object(Body=buf, Bucket='sdg-01',
#                       Key=s3_feedback_path + str(upload_log_id) + "/Feedback.csv")

#         print("data_csv-------------", data_csv.shape[0])
#         if data_csv.shape[0] != 0:

#             data_csv.insert(0, "NOTIFICATION_ID", None)
#             load_file_path = file_path + "/" + file_name

#             data_csv.to_csv(load_file_path, encoding='utf-8', header=True,
#                             doublequote=True, sep=',', index=False, na_rep='\\N', quotechar='"')

#             with conn.cursor() as cur:
#                 load_sql = "LOAD DATA LOCAL INFILE '" + load_file_path + \
#                     "' INTO TABLE PMFBY.COLLECTION_TID FIELDS TERMINATED BY ',' ENCLOSED BY '\"');"
#                 print(load_sql)
#                 cur.execute(load_sql)
#                 conn.commit()

#             os.remove(load_file_path)

#         no_of_records_success = data_csv.shape[0]
#         no_of_records_failed = (no_of_records_uploaded-no_of_records_success)

#         # Update Feedback Available
#         with conn.cursor() as cur:
#             query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 1, PROCESS_END_TIME=now(), FEEDBACK_FILE = 'Feedback.csv', SUCCESS = "+str(no_of_records_success)+", FAILED = " + \
#                 str(no_of_records_failed) + \
#                 "  where id = "+str(upload_log_id)+";"
#             print("Query: ", query)
#             cur.execute(query)
#             conn.commit()

#         pool.release(conn)
#         gc.collect()
#         print("2", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
#         return

#     except Exception as e:
#         print("Exception Occurred")
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#         print(exc_type, fname, exc_tb.tb_lineno)
#         print("Exception: ", e)
#         e_str = str(e).replace('"', '\\"')
#         e_str = e_str.replace("'", "\\'")
#         with conn.cursor() as cur:
#             query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK='Something went wrong', EXCEPTION = '" + \
#                 str(e_str)+"' where id = "+str(upload_log_id)+";"
#             # print("Query: ", query)
#             cur.execute(query)
#             conn.commit()

#         gc.collect()
#         return


# def unknown_process_thread(upload_log_id):
#     try:

#         global pool
#         try:
#             conn = pool.get_conn()
#             with conn.cursor() as cur:
#                 query = "select now()"
#                 cur.execute(query)
#                 n = cur.fetchone()
#         except:
#             print('Trying to reconnect')
#             pool.init()
#             conn = pool.get_conn()

#         # *****database entry*****
#         with conn.cursor() as cur:
#             query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE=2, PROCESS_START_TIME=now(), PROCESS_END_TIME=now(), REMARK='Unknown process', EXCEPTION='Unknown process' where ID=" + \
#                 str(upload_log_id)
#             print(upload_log_id, query)
#             cur.execute(query)
#             conn.commit()

#     except Exception as e:
#         print("Exception Occurred")
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#         print(e, exc_type, fname, exc_tb.tb_lineno)
#         print("Exception: ", e)
#         e_str = str(e).replace('"', '\\"')
#         e_str = e_str.replace("'", "\\'")
#         with conn.cursor() as cur:
#             query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE = 2, PROCESS_END_TIME=now(), REMARK = 'Something went wrong', EXCEPTION = '" + \
#                 str(e_str)+"' where id = "+str(upload_log_id)+";"
#             # print("Query: ", query)
#             cur.execute(query)
#             conn.commit()
#         pool.release(conn)
#         gc.collect()

