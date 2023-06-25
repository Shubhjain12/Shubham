# import json
# import requests
# import zipfile
# import time
# import connectorx as cx
# import os
# import io
# import flask
# import boto3
# from flask import jsonify, send_from_directory, send_file
# from flask import request
# from datetime import datetime, date
# import itertools
# import operator
# #import pymysql
# from flask import Flask,make_response
# from pymysqlpool.pool import Pool
# from flask import Blueprint
# from connection_master import BASE_URL, file_path,file_path1,connection_string2,endpoint,username,password,database_name,connection_string

# reports=Blueprint('reports',__name__)

# #connection_str="mysql://"+str(username)+":"+str(password)+"@"+str(endpoint)+":3306/"+str(database_name)
# pool = Pool(host=endpoint, port=3306, user=username, password=password, db=database_name)
# pool.init()





# #get_notification_details_for_reporting api
# @reports.route('/get_notification_details_for_reporting',methods=['GET'])
# def get_notification_details_for_reporting():
#     try:
#         global pool
#         try:
#             conn=pool.get_conn()
#             with conn.cursor() as cur:
#                 query="select now()"
#                 cur.execute(query)
#                 n=cur.fetchone()
#                 print("connected")
#         except:
#             print('Trying to reconnect')
#             pool.init()
#             conn = pool.get_conn()
#             print(conn) 
#         configid=request.args.get('configid')
#         # fetch notification data l3 to l7 and crop name by configid
#         with conn.cursor() as cur:
#             query = "SELECT L3_NAME, L3_NAME_CODE, L4_NAME, L4_NAME_CODE, L5_NAME, L5_NAME_CODE, L6_NAME, L6_NAME_CODE, L7_NAME, L7_NAME_CODE, CROP_NAME, CROP_CODE FROM pmfby.notification where CONFIGID="+str(configid)+" group by L3_NAME_CODE, L4_NAME_CODE, L5_NAME_CODE, L6_NAME_CODE, L7_NAME_CODE, CROP_CODE order by L3_NAME_CODE, L4_NAME_CODE, L5_NAME_CODE, L6_NAME_CODE, L7_NAME_CODE, CROP_CODE;"
#             print(query)
#             cur.execute(query)
#             results=cur.fetchall()
#         pool.release(conn)
#         # build a key inside key data
#         loc = {}
#         for key, value in itertools.groupby(results, key = operator.itemgetter('L3_NAME')):
#             loc[str(key)] = {}
#             for key1, value1 in itertools.groupby(value, key = operator.itemgetter('L4_NAME')):
#                 loc[str(key)][str(key1)] = {}
#                 for key2, value2 in itertools.groupby(value1, key = operator.itemgetter('L5_NAME')):
#                     loc[str(key)][str(key1)][str(key2)] = {}
#                     for key3, value3 in itertools.groupby(value2, key = operator.itemgetter('L6_NAME')):
#                         loc[str(key)][str(key1)][str(key2)][str(key3)] = {}
#                         for key4, value4 in itertools.groupby(value3, key = operator.itemgetter('L7_NAME')):
#                             loc[str(key)][str(key1)][str(key2)][str(key3)][str(key4)] = {}
#                             print(value4)
#                             loc[str(key)][str(key1)][str(key2)][str(key3)][str(key4)]["CROP"]=[]
#                             for x in value4:
#                                 dict = {
#                                     "CROP_NAME": x['CROP_NAME'],
#                                     "CROP_CODE": x['CROP_CODE']
#                                 }
#                                 loc[str(key)][str(key1)][str(key2)][str(key3)][str(key4)]["CROP"].append(dict)
#                                 loc[str(key)][str(key1)][str(key2)][str(key3)][str(key4)]["L7_CODE"] = x['L7_NAME_CODE']
#                                 loc[str(key)][str(key1)][str(key2)][str(key3)]["L6_CODE"] = x['L6_NAME_CODE']
#                                 loc[str(key)][str(key1)][str(key2)]["L5_CODE"] = x['L5_NAME_CODE']
#                                 loc[str(key)][str(key1)]["L4_CODE"] = x['L4_NAME_CODE']
#                                 loc[str(key)]["L3_CODE"] = x['L3_NAME_CODE']
#          # respose data
#         print(loc)
#         return jsonify(loc) 
#     except Exception as e:
#             print("Exception: ", e)
#             message = {'status': 502,'message': str(e) }
#             respone = jsonify(message)
#             respone.status_code = 502
#             return respone




# #survey api for reports
# @reports.route('/get_survey_data',methods=['GET'])
# def get_survey_data():
#     try:
#     #     global pool
#     #     try:
#     #         conn=pool.get_conn()
#     #         with conn.cursor() as cur:
#     #             query="select now()"
#     #             cur.execute(query)
#     #             n=cur.fetchone()
#     #             print("connected")
#     #     except:
#     #         print('Trying to reconnect')
#     #         pool.init()
#     #         conn = pool.get_conn()
#     #         print(conn)
#         # data=request.json
#         # print(data)
#         # string=''
#         # for i in data:
#         #     string+="c."+i+" in "+str(data[i])+" and "
#         #     #print(i,tuple(data[i]))
#         # string=string.replace("[", "(")
#         # string=string.replace("]", ")")
#         # string=string.replace("c.peril", "b.peril")
#         # print(string[:-4])
#         district=request.args.getlist('district') #l3
#         l4_name=request.args.getlist('l4_name') #l4
#         l5_name=request.args.getlist('l5_name') #l5
#         l6_name=request.args.getlist('l6_name') #l6
#         l7_name=request.args.getlist('l7_name') #l7
#         crop_name=request.args.getlist('crop_name') #crop_name
#         party_id=request.args.getlist('party_id')
#         cover_type=request.args.get('cover_type')
#         start_date=request.args.get('start_date')
#         end_date=request.args.get('end_date')
#         print("--//--",district,l4_name,l5_name,l6_name,l7_name,crop_name,cover_type,start_date)
#         string=''
#         if district:
#             string+="c.district in "+str(district)
#         if l4_name:
#             string+=" AND c.l4_name in "+str(l4_name)
#         if l5_name:
#             string+=" AND c.l5_name in "+str(l5_name)
#         if l6_name:
#             string+=" AND c.l6_name in "+str(l6_name)
#         if l7_name:
#             string+=" AND c.l4_name in "+str(l7_name)
#         if crop_name:
#             string+=" AND c.crop_name in "+str(crop_name)
#         if party_id:
#             string+=" AND c.assigned_partyid in "+str(party_id)
#         if cover_type:
#             string+=" AND a.cover_type="+"'"+str(cover_type)+"'"
#         if start_date is not None and end_date is not None:
#             string+=" AND a.updated_datentime BETWEEN "+"'"+str(start_date)+"'"+ " AND "+"'"+str(end_date)+"'"
#         string=string.replace("[", "(")
#         string=string.replace("]", ")")
#         print("string",string)
       
#         query="""
#                 SELECT * FROM aic_survey.ila as a inner join aic_survey.intimation as b on b.intimationid=a.intimationid
#                 inner join aic_survey.applications as c on c.applicationid=b.applicationid
#                 where  """+string+""";
#                 """
#         print(query) #str(tuple(district)) c.district in
       
#         # query="""
#         #         SELECT * FROM aic_survey.ila as a 
#         #         inner join aic_survey.intimation as b on b.intimationid=a.intimationid     
#         #         inner join aic_survey.applications as c on c.applicationid=b.applicationid
#         #         where c.district in ('Aurangabad') 
#         #         AND a.cover_type = 4 AND a.updated_datentime 
#         #         BETWEEN '2022-11-15' AND '2022-11-20';"""
#         print(query)
#         chunk_df=cx.read_sql(connection_string2,query)
#         print("chunk_df======",chunk_df)

#         zip_file = zipfile.ZipFile(file_path1+"/myzip.zip", 'w')
#         total_row = chunk_df.shape[0]
#         size=200000
#         row = (total_row // size)+1
#         for j in range(row):
#             df = chunk_df[size*j:size*(j+1)]
#             file_name='Survey_reporting_data_sheet'+'_'+str(j+1)+'.csv'
#             df.to_csv(file_name, encoding='utf-8', index=False)
#             zip_file.write(file_name)    
#             os.remove(file_name)
#         zip_file.close()
#         print("END===================")
#         return send_file(file_path1+"/myzip.zip",as_attachment=True)
#     except Exception as e:
#             print("Exception: ", e)
#             message = {'status': 502,'message': str(e) }
#             respone = jsonify(message)
#             respone.status_code = 502
#             return respone








# # @reports.route('/test',methods=['GET'])
# # def test():
# #     global pool
# #     conn=pool.get_conn()
# #     with conn.cursor() as cur:
# #         query="""
# #                 SELECT * FROM pmfby.notification limit 2;
# #                 """
# #         print(query) 
# #         cur.execute(query)
# #         data=cur.fetchall()
       
# #         pool.release(conn)
# #         return jsonify(data) 






# #survey api for reports
# @reports.route('/zip_data',methods=['GET'])
# def zip_data():
#     print("start===================")
#     query="SELECT crop_name, season,year FROM aic_main.notification;"
#     chunk_df=cx.read_sql(connection_string,query)
#     print("chunk_df======",chunk_df)

#     zip_file = zipfile.ZipFile(file_path1+"/myzip.zip", 'w')
#     total_row = chunk_df.shape[0]
#     size=200000
#     row = (total_row // size)+1
#     for j in range(row):
#         df = chunk_df[size*j:size*(j+1)]
#         file_name='Survey_reporting_data'+'_'+str(j+1)+'.csv'
#         df.to_csv(file_name, encoding='utf-8', index=False)
#         zip_file.write(file_name)    
#         os.remove(file_name)
#     zip_file.close()
#     print("END===================")
#     return send_file(file_path1+"/myzip.zip",as_attachment=True)















