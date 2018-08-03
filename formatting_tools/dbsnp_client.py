import sqlite3


class dbSNPclient(object):
    def __init__(self, database='snp.sqlite'):
        self.database = database

    def create_conn(self):
        try:
            conn = sqlite3.connect(self.database)
            return conn
        except NameError as e:
            print(e)
        return None

    def check_orientation_with_dbsnp(self, rsid):
        conn = self.create_conn()
        cur = conn.cursor()
        cur.execute("SELECT strand FROM snp150Common WHERE name=?", (rsid,))
        data = cur.fetchone()
        if data is not None:
            return data[0]
        return False

    def bp_from_rsid(self, rsid):
        conn = self.create_conn()
        cur = conn.cursor()
        cur.execute("SELECT chrom, chromEnd FROM snp150Common WHERE name=?", (rsid,))
        data = cur.fetchone()
        if data is not None:
            return data
        else:        
            return False
