from generate_sumstats_dir_name import *
from formatting_tools.liftover import *


def get_efo_trait(study, database):
    client = OracleGWASClient(database)

    trait_sql = """
    select T.SHORT_FORM
    from EFO_TRAIT T, STUDY S, STUDY_EFO_TRAIT
    where T.ID=STUDY_EFO_TRAIT.EFO_TRAIT_ID
        and S.ID=STUDY_EFO_TRAIT.STUDY_ID
        and S.ACCESSION_ID = :study_acc
    """

    field_dict = {'study_acc': study}
    trait_data = client.query_fetchall(trait_sql, field_dict)
    return trait_data


def parse_filename(filename):
    pmid = None
    study = None
    trait = None
    build = None
    filename = filename.split('/')[-1]
    filename = filename.split('.')[0]
    filename_parts = filename.split('-')

    if len(filename_parts) != 4:
        return False
    else:
        pmid = filename_parts[0]
        study = filename_parts[1]
        trait = filename_parts[2]
        build = filename_parts[3]
    return pmid, study, trait, build


def check_ext(filename):
    parts = filename.split('.')
    if len(parts) == 2 and parts[1] == 'tsv':
        return True
    else:
        return False

def check_build_is_legit(build):
    build_string = build.lower()
    build_number = build_string.replace('build', '')
    if build_number in suffixed_release.keys():
        return True
    else:
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', default='DEV3', choices=['DEV3','SPOTPRO'], 
                        help='Run as (default: DEV3).')
    parser.add_argument('--filename', help='Filename without extension', required=True)

    args = parser.parse_args()
    database = args.database
    filename = args.filename

    pmid_db = None
    trait_db = None

    pmid = None
    study = None
    trait = None
    build = None
    
    if check_ext(filename):

        if parse_filename(filename):
            pmid, study, trait, build = parse_filename(filename)

            try:
                pmid_db = get_publication_data(study, database)[0]
                trait_db = get_efo_trait(study, database)
            except (TypeError, IndexError):
                print('ERROR: Could not find {study} in database'.format(study = study))

            if pmid_db == None or trait_db == None:
                print("ERROR: Can't resolve PMID or trait from {study}".format(study = study))
           
            if pmid_db == pmid and trait in trait_db:
                print("MATCH! File {f} matches {pmid} and {trait} ".format(f=filename, pmid=pmid_db, trait=trait_db))
            else:
                print("ERROR: File {f} does not match {pmid} or {trait} ".format(f=filename, pmid=pmid_db, trait=trait_db))

            if check_build_is_legit(build) is False:
                print("ERROR: {b} is not a valid build".format(b=build))

        else:
            print('ERROR: Expected 4 fields in filename delimitted by "-", but {f} does not conform'.format(f=filename))
    else:
        print('ERROR: Extension is not a tsv')

if __name__ == '__main__':
    main()
