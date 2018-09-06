import cx_Oracle
import contextlib
import sys
#from tqdm import tqdm
import os.path

sys.path.insert(0, 'path/to/gwas_data_sources')
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

    def query(self, sql_string, field_dict):
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
