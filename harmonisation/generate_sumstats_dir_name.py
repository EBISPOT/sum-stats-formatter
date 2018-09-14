import argparse
import sys
import csv

from gwas_oracle_client import OracleGWASClient 


def get_publication_data(study, database):
    client = OracleGWASClient(database)

    publication_sql = """
    select P.PUBMED_ID, REPLACE(A.FULLNAME_STANDARD, ' ', '')
    from STUDY S, PUBLICATION P, AUTHOR A
    where P.FIRST_AUTHOR_ID=A.ID
        and P.PUBMED_ID=S.PUBMED_ID
        and S.ACCESSION_ID = :study_acc
    """

    field_dict = {'study_acc': study}
    publication_data = client.query_fetchone(publication_sql, field_dict)
    return publication_data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', default='DEV3', choices=['DEV3','SPOTPRO'], 
                        help='Run as (default: DEV3).')
    parser.add_argument('--study', help='Study accession', required=True)

    args = parser.parse_args()
    database = args.database
    study = args.study

    if get_publication_data(study, database):
        pmid, author_name = get_publication_data(study, database)
        ss_path = '{author_name}_{pmid}_{study}'.format(\
            author_name = author_name,
            pmid = pmid,
            study = study
            )
        print(ss_path)
    else:
        print('ERROR: Could not find {study} in database'.format(study = study))


if __name__ == '__main__':
    main()
