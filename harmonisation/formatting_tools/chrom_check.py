import csv
import sys
import argparse

from formatting_tools.utils import *
from formatting_tools.sumstats_formatting import *
sys_paths = ['SumStats/sumstats/','../SumStats/sumstats/','../../SumStats/sumstats/']
sys.path.extend(sys_paths)
from common_constants import *


logger = logging.getLogger('chrom_check')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')


def chrom_is_valid(row, header, chromosomes):
    index_chr = header.index(CHR_DSET)
    chromosome = row[index_chr].upper()
    if chromosome in chromosomes:
        return True
    else:
        return chromosome


def main():

    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('--log', help='The name of the log file')
    argparser.add_argument('-config', help='The path to the config.yaml file')
    args = argparser.parse_args()

    file = args.f
    filename = get_filename(file)

    log_file = args.log
    config = args.config
    
    chromosomes = get_chromosome_list(config)

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
                chrom = chrom_is_valid(row, header, chromosomes)
                if chrom is not True:
                    logger.info('Record removed: {0} because chromosome = {1}'.format(row, chrom))

           
if __name__ == "__main__":
    main()
