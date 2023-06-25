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


class Reporting_thread(threading.Thread):
    def __init__(self, user_id, configid, reporting_id, season, product, year, state, method, params):
        threading.Thread.__init__(self)
        self.reporting_id = reporting_id
        self.user_id = user_id
        self.configid = configid
        self.season = season
        self.product = product
        self.year = year
        self.state = state
        self.method = method
        self.params = params

    def run(self):

        # target function of the thread class
        try:
            if self.method in ["PMFBY_Underwriting_Report", "RWBCIS_Underwriting_Report"]:
                pmfby.pmfby_uw_report_thread(self.user_id, self.configid, self.reporting_id,
                                       self.season, self.product, self.year, self.state, self.params)
            elif self.method in ['BSB_Underwriting_Report']:
                pmfby.bsb_uw_report_thread(self.user_id, self.configid, self.reporting_id,
                                       self.season, self.product, self.year, self.state, self.params)
            else:
                pmfby.unknown_report_thread(
                    self.reporting_id)
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
                query = "update PMFBY.REPORTING_LOGS set REPORT_AVAILABLE=2, PROCESS_END_TIME=now(), REMARK='Process stopped abruptly' where REPORTING_ID = " + \
                    str(self.reporting_id)
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
    # 	query = "update PMFBY.FILEUPLOAD_LOGS set FEEDBACK_AVAILABLE=2, PROCESS_END_TIME=now(), REMARK='Process stopped abruptly' where ID = " + str(self.reporting_id)
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
