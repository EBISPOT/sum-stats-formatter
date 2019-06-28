import sys
import csv
import os
import argparse
import logging
import pandas as pd
from pandas_schema import Schema
from validate.schema import *

logging.basicConfig(level=logging.INFO, format='%(name)s (%(levelname)s): %(message)s')
logger = logging.getLogger(__name__)


class Validator:
    def __init__(self, file, stage):
        self.file = file
        self.stage = stage
        self.schema = None
        self.header = []
        self.cols_to_validate = []
        self.cols_to_read = []
        self.sep = get_seperator(self.file) 
        self.bad_rows = []
    
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
        self.check_filename_valid()
        if not self.check_file_is_square():
            logger.error("Please fix the table. Some rows have different numbers of columns to the header")
            logger.info("Rows with different numbers of columns to the header are not validated")
        self.setup_schema()    
        for chunk in self.df_iterator():
            to_validate = chunk[self.cols_to_read]
            to_validate.columns = self.cols_to_validate # sets the headers to standard format if neeeded
            errors = self.schema.validate(to_validate)
            for error in errors:
                logger.error(error)
                self.bad_rows.append(error.row)
                
    def write_valid_lines_to_file(self):
        newfile = self.file + ".valid"
        first_chunk = True
        for chunk in self.df_iterator():
            chunk.drop(self.bad_rows, inplace=True, errors='ignore')
            if first_chunk:
                chunk.to_csv(newfile, mode='w', sep='\t', index=False, na_rep='NA')
                first_chunk = False
            else:
                chunk.to_csv(newfile, mode='a', header=False, sep='\t', index=False, na_rep='NA')

    def check_filename_valid(self):
        if not check_ext(self.file, 'tsv'):
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

    def df_iterator(self):
        df = pd.read_csv(self.file, 
                         sep=self.sep, 
                         dtype=str, 
                         error_bad_lines=False,
                         comment='#', 
                         chunksize=1)
        return df

    def check_file_is_square(self):
        square = True
        with open(self.file) as csv_file:
            dialect = csv.Sniffer().sniff(csv_file.readline())
            csv_file.seek(0)
            reader = csv.reader(csv_file, dialect)
            count = 1
            for row in reader:
                if (len(row) != len(self.header)):
                    logger.error("Length of row {c} is: {l} instead of {h}".format(c=count, l=str(len(row)), h=str(len(self.header))))
                    square = False
                count += 1
        return square

def check_ext(filename, ext):
    filename = filename.split('/')[-1]
    parts = filename.split('.')
    if len(parts) == 2 and parts[-1] == ext:
        return True
    return False

def check_build_is_legit(build):
    build_string = build.lower()
    build_number = build_string.replace('build', '')
    if build_number in BUILD_MAP.keys():
        return True
    return False

def get_seperator(file):
    filename, file_extension = os.path.splitext(file)
    sep =  '\s+'
    if file_extension == '.csv':
        sep = ','
    return sep


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-f", help='The path to the summary statistics file to be validated', required=True)
    argparser.add_argument("--stage", help='The stage in the process the file to be validated is in', default='standard', choices=['curated','standard','harmonised'])
    argparser.add_argument("--logfile", help='Provide the filename for the logs', default='VALIDATE.log')
    argparser.add_argument("--drop-bad-lines", help='Store the good lines from the file in a file named <summary-stats-file>.valid', action='store_true', dest='dropbad')
    
    args = argparser.parse_args()

    logfile = args.logfile
    handler = logging.FileHandler(logfile)
    handler.setLevel(logging.ERROR)
    logger.addHandler(handler)

    validator = Validator(file=args.f, stage=args.stage)
    if not validator.check_filename_valid():
        logger.info("Invalid filename: {}".format(args.f)) 
        logger.info("Exiting before any more checks...")
        sys.exit()
    else:
        logger.info("Filename is good!")
        logger.info("Validating file...")
        validator.validate_data()
        if args.dropbad:
            logger.info("Writing good lines to {}.valid".format(args.f))
            validator.write_valid_lines_to_file()


if __name__ == '__main__':
    main()
