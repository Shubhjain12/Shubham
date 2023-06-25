# Python program raising
# exceptions in a python
# thread

import threading
import ctypes
import time
import pmfby
import account
from connection_master import session, s3, BASE_URL, endpoint, username, password, database_name, connection_string, live_endpoint, live_username, live_password, live_database_name, live_connection_string, file_path
import pymysql
import os
import sys


class SARUS_accounting_thread(threading.Thread):
    def __init__(self,scheduler_log_id):
        threading.Thread.__init__(self)
        self.scheduler_log_id = scheduler_log_id
        
    def run(self):

        # target function of the thread class
        try:
            account.accounting_entry_thread(self.scheduler_log_id)
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
                with conn.cursor() as cur:
                    query6 = "UPDATE `aic_accounts`.`acc_scheduler_log` SET `PROCESS_END_TIME` = now(), `EXECUTION_STATUS`= 0, `REMARKS`= %s WHERE `LOG_ID` = %s;"
                    update_param=[str("Process ended abruptly"),str(self.upload_log_id)]
                    print("Query: ", query6)
                    cur.execute(query6,update_param)
                    
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
