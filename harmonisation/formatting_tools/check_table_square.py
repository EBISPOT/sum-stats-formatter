import csv
import sys
import argparse


from formatting_tools.sumstats_formatting import *
sys_paths = ['SumStats/sumstats/','../SumStats/sumstats/','../../SumStats/sumstats/']
sys.path.extend(sys_paths)
from common_constants import *


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-o', help='The name of the outfile (report)', required=True)
    args = argparser.parse_args()

    file = args.f
    outfile = args.o
    is_header = True
    header_col_count = None

    with open(file) as csv_file, open(outfile, 'a') as outfile:
        csv_reader = get_csv_reader(csv_file)
        for row in csv_reader:
            if is_header:
                header_col_count = len(row)
                outfile.write('Column count from header row: {}.\n'.format(header_col_count))
                is_header = False
            else:
                if len(row) != header_col_count:
                    outfile.write('ERROR: line {ln} has {cc} columns.\n'.format(ln=csv_reader.line_num, cc=len(row)))
                    outfile.write('stopping now and exiting.\n')
                    sys.exit('exiting, please check outfile')
    print(header_col_count)

if __name__ == "__main__":
    main()
