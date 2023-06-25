# Python program raising
# exceptions in a python
# thread

import threading
import ctypes
import time
import pmfby
from connection_master import session, s3, BASE_URL, endpoint, username, password, database_name, connection_string, live_endpoint, live_username, live_password, live_database_name, live_connection_string, file_path
import pymysql
import os
import sys


class SARUS_thread(threading.Thread):
    def __init__(self, user_id, configid, upload_log_id, season, product, year, state, method, state_code):
        threading.Thread.__init__(self)
        self.upload_log_id = upload_log_id
        self.user_id = user_id
        self.configid = configid
        self.season = season
        self.product = product
        self.year = year
        self.state = state
        self.method = method
        self.state_code = state_code

    def run(self):

        # target function of the thread class
        try:
            if self.method in ["PMFBY_Underwriting_Creation", "RWBCIS_Underwriting_Creation"]:
                pmfby.uw_upload_thread(self.user_id, self.configid, self.upload_log_id,
                                       self.season, self.product, self.year, self.state, self.state_code)
            elif self.method in ["PMFBY_Notification_Creation", "RWBCIS_Notification_Creation"]:
                pmfby.notifiction_upload_thread(
                    self.user_id, self.configid, self.upload_log_id, self.season, self.year, self.state, self.state_code)
            elif self.method in ["PMFBY_Underwriting_Endorsement", "RWBCIS_Underwriting_Endorsement"]:
                pmfby.endorsement_upload_thread(
                    self.user_id, self.configid, self.upload_log_id, self.season, self.product, self.year, self.state, self.state_code)
            elif self.method == "BSB_Notification_Creation":
                pmfby.notifiction_upload_thread_bsb(
                    self.user_id, self.configid, self.upload_log_id, self.season, self.year, self.state, self.state_code)
            elif self.method == "PV_Collection_Creation":
                pmfby.pv_collection_upload_thread(
                    self.user_id, self.upload_log_id)
            elif self.method == "TID_Collection_Creation":
                pmfby.tid_collection_upload_thread(
                    self.user_id, self.configid, self.upload_log_id, self.season, self.year, self.state, self.state_code)
            elif self.method == "BSB_Underwriting_Creation":
                pmfby.uw_upload_thread_bsb(self.user_id, self.configid, self.upload_log_id,
                                           self.season, self.product, self.year, self.state, self.state_code)
            elif self.method == "ILA_Intimation_Creation":
                pmfby.ila_intimation(self.upload_log_id)
            else:
                pmfby.unknown_process_thread(
                    self.upload_log_id)
        finally:
            print('ended')

    def get_id(self):

        # returns id of the respective thread
        if hasattr(self, '_thread_id'):
            return self._thread_id
        for id, thread in threading._active.items():
            if thread is self:
                return id

    def raise_exception(self):
        try:
            try:
                conn = pymysql.connect(host=endpoint, user=username, password=password, database=database_name, ssl={
                                       "true": True}, connect_timeout=5, cursorclass=pymysql.cursors.DictCursor, autocommit=True)
            except pymysql.MySQLError as e:
                print("ERROR: Unexpected error: Could not connect to MySQL instance.")
                print(e)
                return
            thread_id = self.get_id()
            res = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id,
                                                             ctypes.py_object(SystemExit))
            print(res)
            if res == 0:
                print("Invalid thread id")
            elif res > 1:
                ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
                print('Exception raise failure')
            else:
                print("thread ended", thread_id)
                query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE=2, PROCESS_END_TIME=now(), REMARK='Process stopped abruptly' where ID = " + \
                    str(self.upload_log_id)
                # print(query)
                with conn.cursor() as cur:
                    cur.execute(query)
                    conn.commit()
            conn.close()
            return
        except Exception as e:
            print("Exception Occurred")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(e, exc_type, fname, exc_tb.tb_lineno)
            print("Exception: ", e)
            return
    # def stop(self, conn):
    # 	print("stopping", self.get_id())
    # 	self._stop_event.set()
    # 	query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE=2, PROCESS_END_TIME=now(), REMARK='Process stopped abruptly' where ID = " + str(self.upload_log_id)
    # 	print(query)
    # 	with conn.cursor() as cur:
    # 		cur.execute(query)
    # 		conn.commit()

# t1 = SARUS_thread('Thread 1')

# t2 = SARUS_thread('Thread 2')
# t3 = SARUS_thread('Thread 3')
# t1.start()
# t2.start()
# t3.start()
# time.sleep(2)
# t1.raise_exception()
# t1.join()
# t2.raise_exception()
# t2.join()
# t3.raise_exception()
# t3.join()
