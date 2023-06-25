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
from custom_thread_accounts import SARUS_accounting_thread
from dfvaliditycheck import CheckValidity

empRow = None
# import aiohttp

account = Blueprint('account', __name__)
LOG = logging.getLogger(__name__)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)


pool = Pool(host=endpoint, port=3306, user=username,
            password=password, db=database_name, local_infile=1)
pool.init()

def accounting_task(scheduler_log_id:int):
    try:
        print("Starting new task in Accounting entries")
        new_task = SARUS_accounting_thread(scheduler_log_id)
        new_task.start()
        new_task.join(7200)
        new_task.raise_exception()
        new_task.join()

        return
    except Exception as e:
        print("Exception Occurred for task id ", scheduler_log_id)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        print("Exception: ", e)
        return


@account.route('/accounting_process', methods=['GET'])
def accounting_process():

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
        process_start_time = datetime.now()
        with conn.cursor() as cur:
            # query4 = "INSERT INTO `aic_accounts`.`acc_scheduler_log` (`METHOD`,`PRE_YEAR_MONTH`,`PROCESS_START_TIME`)VALUES(%s,%s,%s);"
            query4 = "INSERT INTO `aic_accounts`.`acc_scheduler_log` (`METHOD`,`PRE_YEAR_MONTH`,`PROCESS_START_TIME`)SELECT %s , CONCAT ((select SUBSTRING(now() - interval 1 month,1,7) )),%s;"
            pram = ['collection_entry', str(process_start_time)]
            cur.execute(query4, pram)
            scheduler_log_id = cur.lastrowid
        print(f"Accounting SCHEDULER running on {process_start_time}")
        thread = Thread(target=accounting_task, kwargs={
                        'scheduler_log_id': scheduler_log_id})
        thread.start()
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

    except Exception as e:
        print("Accounting SCHEDULER Exception Occurred")
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


def glcode(conn:pymysql.connections.Connection, product:str, state:str):
    '''
        callable function for creating glcode based on product and state
    '''
    try:
        with conn.cursor() as cur:
            # query1 = f'SELECT acode.ACCOUNT_CODE , pcode.PRODUCT_CODE , rcode.RO_OFFICE_CODE, scode.STATE_CODE  FROM aic_accounts.acc_product_code as pcode , aic_accounts.acc_account_codes as acode,aic_accounts.acc_regional_office as rcode inner join aic_accounts.acc_state as scode on rcode.STATE_ID = scode.STATE_ID  where pcode.PRODUCT_NAME=%s and scode.STATE_NAME=%s and acode.ACCOUNT_NAME=%s;'
            query1 = f'SELECT pcode.PRODUCT_CODE , rcode.RO_OFFICE_CODE, scode.STATE_CODE  FROM aic_accounts.acc_product_code as pcode , aic_accounts.acc_regional_office as rcode inner join aic_accounts.acc_state as scode on rcode.STATE_ID = scode.STATE_ID  where pcode.PRODUCT_NAME=%s and scode.STATE_NAME=%s;'
            param = [str(product), str(state)]
            print("param", param)
            cur.execute(query1, param)
            # glcoden = None
            glcoden = cur.fetchone()
            print("n=============", glcoden)

        return glcoden

    except Exception as e:
        print("Exception Occurred for Glociode ")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        return e
    # ,account_type,date,user_name,transcation_code


def transcation_entry(conn:pymysql.connections.Connection, product:str, state:str, account:str, debit_amount:float, credit_amount:float, account_log_id:int,updated_by:int):
    '''
        function for ledger entries based on product, season,account type and amount
    '''
    print("inside transaction entry")
    try:
        Glcode = glcode(conn,product,state)
        print("Glcode=", Glcode)
        modify_AT = datetime.now()
        gl = Glcode['RO_OFFICE_CODE']+'.'+'10'+'.'+account+'.' + \
            Glcode['RO_OFFICE_CODE']+'.'+Glcode['PRODUCT_CODE'] + \
            '.'+Glcode['STATE_CODE']+'.'+'0000'
        print("gl==", gl)
        with conn.cursor() as cur:
            query2 = f'insert into aic_accounts.acc_ledger_entry(GL_Code,ro_office_code,account_code,department_code,inter_office_code,product_code,state_code,debit_amount,credit_amount,modified_by,modified_date,transcation_status,accounting_log_id)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
            print("query2===", query2)
            param1 = [str(gl), str(Glcode['RO_OFFICE_CODE']), account,
                      str(10), str(Glcode['RO_OFFICE_CODE']),
                      str(Glcode['PRODUCT_CODE']), str(Glcode['STATE_CODE']), str(
                          debit_amount), str(credit_amount),
                      updated_by, str(modify_AT), 1, str(account_log_id)]
            print("param1==", param1)
            cur.execute(query2, param1)
            conn.commit()

        return True

    except Exception as e:
        print(" transcation Exception Occurred")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        return e


def accounting_entry_thread(scheduler_log_id:int):
    '''
        Parent function for scheduling scheme-wise accounting entries 
    '''
    conn = pool.get_conn()
    # print("connection type",type(conn))
    print("Inside Collection_entry thread")
    
    try:
        record_data = {"PMFBY_PROPOSAL": "with  accounting as (SELECT pro.GROSS_PREMIUM as GROSS_PREMIUM,pro.GOI_SHAREOF_PREMIUM as SUBSIDY,c.STATE AS STATE, c.PRODUCT AS PRODUCT , pro.CONFIGID as CONFIGID FROM PMFBY.PROPOSAL AS pro inner join PMFBY.PRODUCT_CONFIG as c on pro.CONFIGID=c.CONFIGID where  YEAR(pro.UPDATED_DATE) = YEAR(DATE_SUB(NOW(), INTERVAL 1 MONTH)) AND MONTH(pro.UPDATED_DATE) = MONTH(DATE_SUB(NOW(), INTERVAL 1 MONTH)) UNION SELECT pro1.GROSS_PREMIUM as GROSS_PREMIUM,pro1.GOI_SHAREOF_PREMIUM as SUBSIDY,c1.STATE AS STATE, c1.PRODUCT AS PRODUCT , pro1.CONFIGID as CONFIGID FROM PMFBY.ENDORSEMENT_HISTORY AS pro1 inner join PMFBY.PRODUCT_CONFIG as c1 on pro1.CONFIGID=c1.CONFIGID where  YEAR(pro1.UPDATED_DATE) = YEAR(DATE_SUB(NOW(), INTERVAL 1 MONTH)) AND MONTH(pro1.UPDATED_DATE) = MONTH(DATE_SUB(NOW(), INTERVAL 1 MONTH))) select sum(GROSS_PREMIUM) as GROSS_PREMIUM,sum(SUBSIDY) as SUBSIDY,CONFIGID,STATE,PRODUCT from accounting group by CONFIGID"}
        for key, value in record_data.items():
            entry_dataframe = pd.read_sql(value, read_connection_string)
            # *************** calculating premium ************** 
            ledger_return=ledger_entries(conn,key,entry_dataframe,updated_by=-1,scheduler_log_id=scheduler_log_id) 
            if ledger_return != True:
                    raise Exception(ledger_return)
            with conn.cursor() as cur:
                query6 = "UPDATE `aic_accounts`.`acc_scheduler_log` SET `PROCESS_END_TIME` = now(), `EXECUTION_STATUS`= 1 WHERE `LOG_ID` = %s;"
                cur.execute(query6, [str(scheduler_log_id)])
                conn.commit()
            pool.release(conn)
    except Exception as e:
        print("Some exception", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        e_str = str(e)

        try:
            with conn.cursor() as cur:
                query6 = "UPDATE `aic_accounts`.`acc_scheduler_log` SET `PROCESS_END_TIME` = now(), `EXECUTION_STATUS`= 0, `REMARKS`= %s WHERE `LOG_ID` = %s;"
                update_param = [str(e_str), str(scheduler_log_id)]
                print("Query: ", update_param)
                cur.execute(query6, update_param)
                conn.commit()

        except Exception as e1:
            print("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE-", e1)
        finally:
            pool.release(conn)

def ledger_entries(conn:pymysql.connections.Connection,method:str,entry_dataframe:pd.DataFrame,updated_by:int=-1,scheduler_log_id:int=None):
    '''
       function for accounting entries based on values provided in the "entry_dataframe"
    '''
    try:
        update_date = datetime.now()    
        with conn.cursor() as cur:
            query7 = "INSERT INTO `aic_accounts`.`acc_accounting_log`(`SCHEDULAR_LOG_ID`,`METHOD_NAME`,`UPDATED_BY`,`UPDATED_DATE`)VALUES(%s,%s,%s,%s);"
            log_pram = [scheduler_log_id,str(method),updated_by,str(update_date)]
            cur.execute(query7, log_pram)
            account_log_id = cur.lastrowid

            query1 = f'SELECT * FROM aic_accounts.acc_account_activity where ACTIVITY_TABLE=%s;'
            param = [str(method)]
            cur.execute(query1, param)
            conn.commit()
            activity_record = cur.fetchall()
            print("activity_record fetched")
        #************ reading proposal Data ***************
        
        # data = pd.concat([pd.read_sql(q, read_connection_string) for q in value])
        # prposal_dataframe=data.groupby(['CONFIGID','STATE','PRODUCT']).sum()
        print("table data fetched for acc entries")
        #******** loop for Activity wise debit and credit ************
        for k_data, v_data in entry_dataframe.iterrows():
            for i in activity_record:
                # x=pro_data[activity_record[i]['CREDIT']]
                if i['DEBIT'] != None:
                    if v_data[str(i['DEBIT'])] > 0:
                        debit_amount = v_data[str(i['DEBIT'])]
                    else:
                        credit_amount = v_data[str(i['DEBIT'])]
                else:
                    debit_amount = 0.0

                if i['CREDIT'] != None:
                    if v_data[str(i['CREDIT'])] > 0:
                        credit_amount = v_data[str(i['CREDIT'])]
                    else:
                        debit_amount = v_data[str(i['CREDIT'])]
                else:
                    credit_amount = 0.0
                account_code = i['ACCOUNT_CODE']
                state = v_data['STATE']
                product = v_data['PRODUCT']
                # state=str(k_data[1])
                # product=str(k_data[2])
                #*********** Error handling ************
                transcation_return = transcation_entry(
                    conn, product, state, account_code, debit_amount, credit_amount, account_log_id,updated_by)
                if transcation_return != True:
                    raise Exception(transcation_return)
        with conn.cursor() as cur:
            query8 = "UPDATE `aic_accounts`.`acc_accounting_log` SET `STATUS` = 1 WHERE `ACCOUNTING_LOG_ID` = %s;"
            cur.execute(query8, [str(account_log_id)])
            conn.commit()
        
        return True
        
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(e, exc_type, fname, exc_tb.tb_lineno)
        e_str = str(e)
        with conn.cursor() as cur:
                query6 = "UPDATE `aic_accounts`.`acc_accounting_log` SET `STATUS` = 0, `REMARK`= %s WHERE `ACCOUNTING_LOG_ID` = %s;"
                update_param = [str(e_str), str(account_log_id)]
                print("Query: ", update_param)
                cur.execute(query6, update_param)
                conn.commit()
        return e  
    



    