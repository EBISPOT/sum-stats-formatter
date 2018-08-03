import csv
import sys
import argparse
sys_paths = ['/nfs/spot/data/test/sumstats/formatting_tools/sumstats/','sumstats/','../sumstats/','../../sumstats/']
sys.path.extend(sys_paths)
from common_constants import *

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
    # or range
    'range': RANGE_DSET,
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


def map_x_y_to_23_24(row, header):
    index_chr = header.index(CHR_DSET)
    chromosome = row[index_chr].lower()
    if 'x' in chromosome:
        chromosome = '23'
    if 'y' in chromosome:
        chromosome = '24'
    row[index_chr] = chromosome
    return row


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-d', help='The name of the output directory', required=True)
    args = argparser.parse_args()

    file = args.f
    outdir = args.d
    filename = get_filename(file)
    what_changed = None
    new_header = None
    is_header = True
    headers_to_add = []
#    lines = []

    with open(file) as csv_file:
        csv_reader = get_csv_reader(csv_file)
        result_file = open(outdir + filename + ".tsv", "w")
        writer = csv.writer(result_file, delimiter='\t')

        for row in csv_reader:
            if is_header:
                what_changed = mapped_headers(row[:])
                new_header = refactor_header(row)
                headers_to_add = missing_headers(new_header)
                new_header.extend(headers_to_add)
                is_header = False
                writer.writerow(new_header)
            else:
                row = add_blank_col_to_row(row, headers_to_add)
                #row = map_x_y_to_23_24(row, new_header)
                writer.writerow(row)
        

#    new_filename = 'sumstats_' + filename + '.tsv'
#    with open(new_filename, 'w') as result_file:
#        writer = csv.writer(result_file, delimiter='\t')
#        writer.writerows([new_header])
#        writer.writerows(lines)

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
