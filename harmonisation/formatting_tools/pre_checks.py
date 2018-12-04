import csv
import sys
import argparse

from formatting_tools.sumstats_formatting import *
sys_paths = ['SumStats/sumstats/','../SumStats/sumstats/','../../SumStats/sumstats/']
sys.path.extend(sys_paths)
from common_constants import *


def check_for_header_ambigs(header):
    header_ambigs = []
    for h in header:
        if h in TO_LOAD_DSET_HEADERS_DEFAULT and h not in sumstat_header_transformations.keys():
            header_ambigs.append(h)
    return header_ambigs
            

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-o', help='The name of the outfile (report)', required=True)
    args = argparser.parse_args()

    file = args.f
    outfile = args.o
    filename = get_filename(file)
    header = None
    header_col_count = None

    with open(file) as csv_file, open(outfile, 'a') as outfile:
        csv_reader = get_csv_reader(csv_file)
        header = next(csv_reader) 
        header_col_count = len(header)
        outfile.write('Column count from header row: {}.\n'.format(header_col_count))
        header_ambigs = check_for_header_ambigs(header)
        if len(header_ambigs) > 0:
            outfile.write("ERROR: The following headers are ambiguous:\n{}\n".format(','.join(header_ambigs)))
        for row in csv_reader:
            if len(row) != header_col_count:
                outfile.write('ERROR: line {ln} has {cc} columns.\n'.format(ln=csv_reader.line_num, cc=len(row)))
                outfile.write('stopping now and exiting - check the whole table.\n')
                sys.exit('exiting, please check outfile')


if __name__ == "__main__":
    main()
