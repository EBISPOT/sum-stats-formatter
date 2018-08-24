import csv
import sys
import argparse

sys_paths = ['SumStats/sumstats/','../SumStats/sumstats/','../../SumStats/sumstats/']
sys.path.extend(sys_paths)
from sumstats_formatting import *
from common_constants import *


logger = logging.getLogger('chrom_check')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')


CHROMOSOMES = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', 'X', 'Y']


def chrom_is_valid(row, header):
    index_chr = header.index(CHR_DSET)
    chromosome = row[index_chr].upper()
    if chromosome in CHROMOSOMES:
        return True
    else:
        return chromosome


def main():

    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('--log', help='The name of the log file')
    args = argparser.parse_args()

    file = args.f
    filename = get_filename(file)

    log_file = args.log

    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    header = None
    is_header = True
    
    print(CHR_DSET)
    with open(file) as csv_file:
        csv_reader = get_csv_reader(csv_file)
        for row in csv_reader:
            if is_header:
                header = row
                is_header = False
            else:
                chrom = chrom_is_valid(row, header)
                if chrom is not True:
                    logger.info('Record removed: {0} because chromosome = {1}'.format(row, chrom))

           
if __name__ == "__main__":
    main()
