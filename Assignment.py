import json
import requests
import time
import connectorx as cx
import pandas as pd
import os
import io
import flask
import boto3
from flask import jsonify, send_from_directory, send_file
from flask import request
from datetime import datetime, date
#import pymysql
from flask import Flask, make_response
from pymysqlpool.pool import Pool
from flask import Blueprint
# from connection_master import BASE_URL, file_path,read_connection,endpoint,username,password,database_name,connection_string2
from connection_master import BASE_URL, file_path, connection_string, endpoint, username, password, database_name, live_connection_string, live_username, live_password, live_endpoint, live_database_name


assignment = Blueprint('assignment', __name__)


# connection for PMFBY
# connection_str="mysql://"+str(username)+":"+str(password)+"@"+str(endpoint)+":3306/"+str(database_name)
pool = Pool(host=endpoint, port=3306, user=username,
            password=password, db=database_name)

pool.init()

pool_1 = Pool(host=live_endpoint, port=3306, user=live_username,
            password=live_password, db=live_database_name)

pool_1.init()

# # SHAHID


# #ILA Assignment api changed
@assignment.route('/ILA_Assignment', methods=['GET'])
def ILA_Assignment():
    get_time = datetime.now()
    print("************* ILA_Assignment Main function Start *************")
    try:
        get_times = datetime.now()
        # print("after conn:",get_times)
        product_type = request.args.get('product_type')
        configid = request.args.get('configid')
        print("configid", configid, 'product_type', product_type,
              type(product_type), str(product_type))

        if product_type == "PMFBY":

            query1 = """with dist as (SELECT NOTIFICATION_ID, L3_NAME as district, L3_NAME_CODE as district_code, L4_NAME as block_name, L4_NAME_CODE as block_code, CONFIGID FROM PMFBY.NOTIFICATION where CONFIGID="""+configid+""" group by L3_NAME, L4_NAME, CONFIGID) 
            select dist.NOTIFICATION_ID, dist.district, dist.district_code, dist.block_name, dist.block_code, assigned_task.PARTY_ID as party_id , assigned_task.ASSIGNED_BY, assigned_task.ASSIGNED_ON,
            CASE
                WHEN assigned_task.PARTY_ID is NULL THEN 'UNASSIGNED'
                ELSE 'ASSIGNED'
            END AS status
            from dist
            left join AIC_SURVEY.ILA_PARTY_ASSIGNMENT as assigned_task on assigned_task.CONFIGID=dist.CONFIGID and assigned_task.L4_NAME=dist.block_name
            where dist.CONFIGID="""+configid+""";"""

            print(query1)
            data = cx.read_sql(connection_string, query1)
            print("query1 Data Pre:----------- \n", data)
            data['party_id'] = data['party_id'].fillna(value=0)
            data['ASSIGNED_BY'] = data['ASSIGNED_BY'].fillna(value=0)
            # data['ASSIGNED_ON'] = data['ASSIGNED_ON'].fillna(value=0)
            data['district_code'] = data['district_code'].astype(int)
            data['block_code'] = data['block_code'].astype(int)
            print("query1 Data Post:----------- \n", data)

            query2 = "SELECT name as party_name,party_id FROM party_management.party_detail;"
            # print(query2)
            partyid_name = cx.read_sql(live_connection_string, query2)
            print("\n", partyid_name)

            # merge party name where party_id match
            final_data = pd.merge(data, partyid_name, on=[
                                  'party_id'], how='left').fillna(0)
            final_data['party_id'] = final_data['party_id'].astype(int)
            final_data = final_data.replace(0, None)
            final_data['ASSIGNED_BY'] = final_data['ASSIGNED_BY'].fillna(
                value=0)
            print("final_data:----------- \n", final_data)

            # convert  df to dict
            dict_data = final_data.to_dict(orient='records')
            print(dict_data)
            return jsonify(dict_data)

    except Exception as e:
        print("Exception: ", e)
        message = {'status': 502, 'message': str(e)}
        respone = jsonify(message)
        respone.status_code = 502
        return respone

# get_party_id api


@assignment.route('/get_party_id', methods=['GET'])
def get_party_id():
    print("************* get_party_id Start *************")
    try:
        global pool_1
        try:
            conn = pool_1.get_conn()
            with conn.cursor() as cur:
                query = "select now()"
                cur.execute(query)
                n = cur.fetchone()
                # print("connected")
        except:
            # print('Trying to reconnect')
            pool_1.init()
            conn = pool_1.get_conn()
            # print(conn)
        with conn.cursor() as cur:
            query = "SELECT party_id,name FROM party_management.party_detail;"
            # print(query)
            cur.execute(query)
            data = cur.fetchall()
        pool_1.release(conn)
        return jsonify(data)
    except Exception as e:
        print("Exception: ", e)
        message = {'status': 502, 'message': str(e)}
        respone = jsonify(message)
        respone.status_code = 502
        return respone

# ILA ILA_Assigned_task api


@assignment.route('/ILA_Assigned_task', methods=['POST'])
def ILA_Assigned_task():
    get_time = datetime.now()
    print("************* ILA_Assigned_task Start *************")
    try:
        global pool
        try:
            conn = pool.get_conn()
            with conn.cursor() as cur:
                query = "select now()"
                cur.execute(query)
                n = cur.fetchone()
                # print("connected")
        except:
            # print('Trying to reconnect')
            pool.init()
            conn = pool.get_conn()
            # print(conn)
        get_times = datetime.now()
        # print("start",get_times)
        # user_name = request.args.get('username')
        user_id = request.args.get('user_id')
        product_type = request.args.get('product_type')
        configid = request.args.get('configid')
        partyid = request.args.get('partyid')
        # district=request.args.getlist('district')
        block_name = request.args.getlist('block_name')
        block_name_2 = [eval(s) for s in block_name]
 
        # block_name = [['HARIDWAR_1','NARSAN'], ['HARIDWAR_2','LALDHANG']]
        print("block_name-----",block_name_2)
        print("user_name-----",user_id)

        bulk_insert = ""
        for i in block_name_2:
            with conn.cursor() as cur:
                query = "SELECT * from AIC_SURVEY.ILA_PARTY_ASSIGNMENT where CONFIGID=" + \
                    str(configid)+" and L3_NAME='"+i[0]+"' and L3_NAME_CODE='"+i[1]+"' and L4_NAME='"+i[2]+"';"
                print(query)
                cur.execute(query)
                exists_data = cur.fetchall()
                # check data exists or not
            if not exists_data:
                bulk_insert += "("+"'"+str(configid)+"'"+","+"'" + \
                    user_id+"'"+","+"'"+partyid+"'"+","+"'"+str(i[0])+"'"+","+"'"+str(i[1])+"'"+","+"'"+str(i[2])+"'),"

        bulk_insert = bulk_insert.strip(',')
        print("bulk_insert \n", bulk_insert)
        if bulk_insert:
            with conn.cursor() as cur:
                query="insert into AIC_SURVEY.ILA_PARTY_ASSIGNMENT (CONFIGID,ASSIGNED_BY,PARTY_ID,L3_NAME,L3_NAME_CODE,L4_NAME)values"+bulk_insert+";"
                print(query)
                cur.execute(query)
                conn.commit()
        pool.release(conn)

        message = {'status': 200, 'message': 'AAsigned'}
        respone = jsonify(message)
        respone.status_code = 200
        return respone
    except Exception as e:
        print("Exception: ", e)
        message = {'status': 502, 'message': str(e)}
        respone = jsonify(message)
        respone.status_code = 502
        return respone


# ILA ILA_UNassigned_task api
@assignment.route('/ILA_UNassigned_task', methods=['POST'])
def ILA_UNassigned_task():
    get_time = datetime.now()
    print("************* ILA_UNAssigned_task Start *************")
    try:
        global pool
        try:
            conn = pool.get_conn()
            with conn.cursor() as cur:
                query = "select now()"
                cur.execute(query)
                n = cur.fetchone()
                # print("connected")
        except:
            #print('Trying to reconnect')
            pool.init()
            conn = pool.get_conn()
            # print(conn)
        get_times = datetime.now()
        # print("start",get_times)
        # product_type=request.args.get('product_type')
        configid = request.args.get('configid')
        # partyid=request.args.get('partyid')
        # district=request.args.getlist('district')
        block_name = request.args.getlist('block_name')
        block_name_2 = [eval(s) for s in block_name]
        # print(product_type,district,type(district))
        print("configid------- : ", configid)
        # print("partyid-------- : ",partyid)
        print("block_name-------- : ", block_name)
        block_list = ""
        for blk in block_name_2:
            # block_list += "'"+str(blk[2])+"',"
            block_list += "("+"'"+str(configid)+"'"+","+"'"+str(blk[0])+"'"+","+"'"+str(blk[1])+"'"+","+"'"+str(blk[2])+"'),"


        block_list = block_list.strip(',')
        print("block_list : ", block_list)
        with conn.cursor() as cur:
            # query = "DELETE from AIC_SURVEY.ILA_PARTY_ASSIGNMENT where CONFIGID=" + \
            #     str(configid)+" and L4_NAME in ("+block_list+");"
            query = "DELETE from AIC_SURVEY.ILA_PARTY_ASSIGNMENT where (CONFIGID,L3_NAME,L3_NAME_CODE,L4_NAME)IN ("+block_list+");"

            print("Delete",query)
            cur.execute(query)
            conn.commit()
        pool.release(conn)
        # Response
        message = {'status': 200, 'message': 'UNassigned successfuly'}
        respone = jsonify(message)
        respone.status_code = 200
        return respone
    except Exception as e:
        print("Exception: ", e)
        message = {'status': 502, 'message': str(e)}
        respone = jsonify(message)
        respone.status_code = 502
        return respone
