import argparse
import logging
import logging.config
import sys
import pandas as pd
from io import StringIO
from pandas_schema import Column, Schema
from pandas_schema.validation import LeadingWhitespaceValidation, TrailingWhitespaceValidation, CanConvertValidation, MatchesPatternValidation, InRangeValidation, InListValidation

sys_paths = ['SumStats/sumstats/','../SumStats/sumstats/','../../SumStats/sumstats/']
sys.path.extend(sys_paths)
from common_constants import *


#schema = Schema([
#    Column('Given Name', [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()]),
#    Column('Family Name', [LeadingWhitespaceValidation(), TrailingWhitespaceValidation()]),
#    Column('Age', [InRangeValidation(0, 120)]),
#    Column('Sex', [InListValidation(['Male', 'Female', 'Other'])]),
#    Column('Customer ID', [MatchesPatternValidation(r'\d{4}[A-Z]{4}')])
#])

schema = Schema([
    Column()
])

class Validator:
    def __init__(self, file):
        self.file = file
        self.schema = None

    def validate(self):
        pass

#errors = schema.validate(test_data)

#for error in errors:
#    print(error)

def main():
    logging.config.fileConfig(file_or_resource('logging.ini'),
                              disable_existing_loggers=False)
    logger = logging.getLogger(__name__)

    argparser = argparse.ArgumentParser()
    argparser.add_argument("-f", help='The path to the summary statistics file to be validated', required=True)

    args = argparser.parse_args()

    validator = Validator(file=args.f)
    validator.validate()

if __name__ == '__main__':
    sys.exit(main())
