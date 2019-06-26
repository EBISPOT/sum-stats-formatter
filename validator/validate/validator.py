import sys
import os
import argparse
import logging
import logging.config
import pandas as pd
from pandas_schema import Schema
from validate.schema import *


class Validator:
    def __init__(self, file, stage):
        self.file = file
        self.stage = stage
        self.schema = None
        self.header = []
        self.cols_to_validate = []
        self.cols_to_read = []
        self.sep = self.get_seperator()    
    

    def get_seperator(self):
        filename, file_extension = os.path.splitext(self.file)
        sep =  '\s+'
        if file_extension == '.csv':
            sep = ','
        return sep

    def setup_schema(self):
        if self.stage == 'curated':
            self.cols_to_validate = [CURATOR_STD_MAP[h] for h in self.header if h in CURATOR_STD_MAP]
            self.cols_to_read = [h for h in self.header if h in CURATOR_STD_MAP]
        else:
            self.cols_to_validate = [h for h in self.header if h in STD_COLS]
            self.cols_to_read = [h for h in self.header if h in STD_COLS]
        self.schema = Schema([VALIDATORS[h] for h in self.cols_to_validate])

    def get_header(self):
        first_row = pd.read_csv(self.file, sep=self.sep, comment='#', nrows=1, index_col=False)
        return first_row.columns.values

    def validate_data(self):
        self.header = self.get_header()
        self.setup_schema()    
        df = pd.read_csv(self.file, sep=self.sep, dtype=str, usecols=self.cols_to_read, index_col=False, comment='#')
        df.columns = self.cols_to_validate # sets the headers to standard format if neeeded
        errors = self.schema.validate(df)
        for error in errors:
            print(error)

    def check_filename_valid(self):
        if not check_ext_is_tsv(self.file):
            return False
        pmid = None
        study = None
        trait = None
        build = None
        filename = self.file.split('/')[-1]
        filename = filename.split('.')[0]
        filename_parts = filename.split('-')

        if len(filename_parts) != 4:
            return False
        else:
            pmid = filename_parts[0]
            study = filename_parts[1]
            trait = filename_parts[2]
            build = filename_parts[3]

        if not check_build_is_legit(build):
            return False
        return True


def check_ext_is_tsv(filename):
    filename = filename.split('/')[-1]
    parts = filename.split('.')
    if len(parts) == 2 and parts[-1] == 'tsv':
        return True
    else:
        return False

def check_build_is_legit(build):
    suffixed_release = {'28': 'NCBI28',
                        '29': 'NCBI29',
                        '30': 'NCBI30',
                        '31': 'NCBI31',
                        '33': 'NCBI33',
                        '34': 'NCBI34',
                        '35': 'NCBI35',
                        '36': 'NCBI36',
                        '37': 'GRCh37',
                        '38': 'GRCh38'}
    build_string = build.lower()
    build_number = build_string.replace('build', '')
    if build_number in suffixed_release.keys():
        return True
    else:
        return False


def main():
    #logging.config.fileConfig(file_or_resource('logging.ini'),
    #                          disable_existing_loggers=False)
    #logger = logging.getLogger(__name__)

    argparser = argparse.ArgumentParser()
    argparser.add_argument("-f", help='The path to the summary statistics file to be validated', required=True)
    argparser.add_argument("--stage", help='The stage in the process the file to be validated is in', default='standard', choices=['curated','standard','harmonised'])

    args = argparser.parse_args()

    validator = Validator(file=args.f, stage=args.stage)
    if not validator.check_filename_valid():
        print("Invalid filename: {}".format(args.f)) 
        print("Exiting before any more checks...")
        sys.exit()
    else:
        print("Filename is good!")
        print("Validating file...")
        validator.validate_data()


if __name__ == '__main__':
    main()
