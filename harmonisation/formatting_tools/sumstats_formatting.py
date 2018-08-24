import csv
import sys
import argparse
import logging

sys_paths = ['SumStats/sumstats/','../SumStats/sumstats/','../../SumStats/sumstats/']
sys.path.extend(sys_paths)
from common_constants import *


logger = logging.getLogger('sumstats_formatting')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')


sumstat_header_transformations = {

    # variant id
    'snp': SNP_DSET,
    # p-value
    'pval': PVAL_DSET,
    # chromosome
    'chr': CHR_DSET, 
    # base pair location
    'bp': BP_DSET, 
    # odds ratio
    'or': OR_DSET,
    # ci lower
    'ci_lower': RANGE_L_DSET,
    # ci upper
    'ci_upper': RANGE_U_DSET,
    # beta
    'beta': BETA_DSET,
    # standard error
    'se': SE_DSET,
    # effect allele
    'effect_allele': EFFECT_DSET,
    # other allele
    'other_allele': OTHER_DSET,
    # effect allele frequency
    'eaf': FREQ_DSET
}


BLANK_SET = {'', ' ', '-', '.', 'na', None, 'none', 'nan', 'nil'}


def refactor_header(header):
    return [sumstat_header_transformations[h] if h in sumstat_header_transformations else h for h in header]


def mapped_headers(header):
    return {h: sumstat_header_transformations[h] for h in header if h in sumstat_header_transformations}


def missing_headers(header):
    return [h for h in sumstat_header_transformations.values() if h not in header]


def get_csv_reader(csv_file):
    dialect = csv.Sniffer().sniff(csv_file.readline())
    csv_file.seek(0)
    return csv.reader(csv_file, dialect)


def get_filename(file):
    return file.split("/")[-1].split(".")[0]


def add_blank_col_to_row(row, headers_to_add):
    for _ in headers_to_add:
        row.append('NA')
    return row


def blanks_to_NA(row):
    for n, i in enumerate(row):
        if i.lower() in BLANK_SET:
            row[n] = 'NA'
    return row


def map_23_24_to_x_y(row, header):
    index_chr = header.index(CHR_DSET)
    chromosome = row[index_chr]
    if chromosome == '23':
        chromosome = 'X'
    if chromosome == '24':
        chromosome = 'Y'
    row[index_chr] = chromosome
    return row


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-d', help='The name of the output directory', required=True)
    argparser.add_argument('--log', help='The name of the log file')
    args = argparser.parse_args()

    file = args.f
    outdir = args.d
    log_file = args.log
    filename = get_filename(file)
    what_changed = None
    new_header = None
    is_header = True
    headers_to_add = []

    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    with open(file) as csv_file:
        csv_reader = get_csv_reader(csv_file)
        result_file = open(outdir + filename + ".tsv", "w")
        writer = csv.writer(result_file, delimiter='\t')

        for row in csv_reader:
            if is_header:
                what_changed = mapped_headers(row[:])
                new_header = refactor_header(row)
                headers_to_add = missing_headers(new_header)
                logger.info('headers missing and added: {}'.format(headers_to_add))
                new_header.extend(headers_to_add)
                is_header = False
                writer.writerow(new_header)
            else:
                row = add_blank_col_to_row(row, headers_to_add)
                row = map_23_24_to_x_y(row, new_header)
                row = blanks_to_NA(row)
                writer.writerow(row)
        

    print("\n")
    print("------> Output saved in file:", outdir + filename + ".tsv", "<------")
    print("\n")
    print("Showing how the headers where mapped below...")
    print("\n")
    for key, value in what_changed.items():
        print(key, " -> ", value)
    print("\n")
    if headers_to_add:
        print("The following headers were missing, and have been added with 'NA' values:")
        print("\n")
        for h in headers_to_add:
            print(h)
    else:
        print("All required headers were found in input file.")


if __name__ == "__main__":
    main()
