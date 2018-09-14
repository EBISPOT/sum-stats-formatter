# Activate Python venv for the script - uncomment to run script on commandline
activate_this_file = "path/to/activate_this"
#execfile(activate_this_file, dict(__file__ = activate_this_file))
exec(open(activate_this_file).read(), {'__file__': activate_this_file})

import cx_Oracle
import contextlib
import sys
#from tqdm import tqdm
import os.path

sys.path.insert(0, 'path/to/gwas_data_source')
import gwas_data_sources


class OracleGWASClient(object):
    def __init__(self, database):
        self.ip, self.port, self.sid, self.username, self.password = \
            gwas_data_sources.get_db_properties(database) 

    def create_conn(self):
        try:
            dsn_tns = cx_Oracle.makedsn(self.ip, self.port, self.sid)
            connection = cx_Oracle.connect(self.username, self.password, dsn_tns)
            return connection
        except cx_Oracle.DatabaseError as exception:
            print(exception)

    def query_fetchone(self, sql_string, field_dict):
        connection = self.create_conn()
        with contextlib.closing(connection.cursor()) as cursor:
            cursor.prepare(sql_string)
            cursor.execute(None, field_dict)
            data = cursor.fetchone()
            if data is not None:
                return data
            else:
                return False
        connection.close()

    def query_fetchall(self, sql_string, field_dict):
        connection = self.create_conn()
        with contextlib.closing(connection.cursor()) as cursor:
            cursor.prepare(sql_string)
            cursor.execute(None, field_dict)
            datalist = [item[0] for item in cursor.fetchall()]
            if datalist is not None:
                return datalist
            else:
                return False
        connection.close()
