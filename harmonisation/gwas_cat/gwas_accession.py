import argparse
import sys
import csv

from gwas_oracle_client import OracleGWASClient 


def insert_gcst_and_ext_id_to_gwas_database(extid, gcst, database):
    client = OracleGWASClient(database)
    insert_sql = """
        insert into GCST_ACCESSION_EXTERNAL(ACCESSION_ID, EXTERNAL_ID) 
        VALUES (:gcst, :extid)
        """
    field_dict = {'gcst': gcst, 'extid': extid}
    client.execute_commit(insert_sql, field_dict)


def generate_new_gcst_accession(database):
    number_string = str(get_new_accession_suffix(database))
    correct_number_length = 6
    while len(number_string) < correct_number_length:
        number_string = '0' + number_string
    return 'GCST' + number_string


def get_new_accession_suffix(database):
    client = OracleGWASClient(database)
    trigger_new_gcst_sql = 'select accession_seq.NEXTVAL from dual'
    gcst_accession_response = client.execute_no_params(trigger_new_gcst_sql)
    gcst_accession = parse_response(gcst_accession_response)
    return gcst_accession


def parse_response(database_response):
    return database_response[0]


def select_entry(database, gcst):
    client = OracleGWASClient(database)
    sql = 'select * from GCST_ACCESSION_EXTERNAL G where G.ACCESSION_ID = :gcst '
    field_dict = {'gcst': gcst}
    return client.query_fetchone(sql, field_dict) 


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', default='SPOTPRO', choices=['CTTV','SPOTPRO'], 
                        help='Run as (default: SPOTPRO).')
    parser.add_argument('-extid', help='The external id you want to assign a GCST accession to', required=True)

    args = parser.parse_args()
    database = args.database
    extid = args.extid

    gcst = generate_new_gcst_accession(database)
    insert_gcst_and_ext_id_to_gwas_database(extid, gcst, database)
    print(select_entry(database, gcst))


if __name__ == '__main__':
    main()
