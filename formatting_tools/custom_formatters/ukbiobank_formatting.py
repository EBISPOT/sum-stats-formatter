import csv
import argparse
import sys

sys_paths = ['bin/final_formatting/','../bin/final_formatting/','../../bin/final_formatting/','../../../bin/final_formatting/']
sys.path.extend(sys_paths)
from sumstats_formatting import *


uk_biobank_header_transformations = {

    # variant id
    'rsid': 'snp',
    # p-value
    'pval': 'pval',
    # chromosome
    'chr': 'chr',
    # base pair location
    'bp': 'bp',
    # beta
    'beta': 'beta',
    # standard error
    'se': 'se',
    # effect allele
    'alt': 'effect_allele',
    # other allele
    'ref': 'other_allele'
}


def refactor_ukbb_header(header):
    header.remove('variant')
    variant_split_headers = ['chr', 'bp', 'ref', 'alt']
    complete_header = variant_split_headers + header
    return [uk_biobank_header_transformations[h] if h in uk_biobank_header_transformations else h for h in complete_header]


def split_col(row):
    col1 = row.pop(0).split(':')
    formatted_row = col1 + row
    return formatted_row


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    args = argparser.parse_args()

    file = args.f
    filename = get_filename(file)
    new_header = None
    is_header = True

    lines = []
    with open(file) as csv_file:
        csv_reader = get_csv_reader(csv_file)
        for row in csv_reader:
            if is_header:
                # bring to common format:
                new_header = refactor_ukbb_header(row)
                # bring to intermediate format:
                new_header = refactor_header(new_header)
                headers_to_add = missing_headers(new_header)
                new_header.extend(headers_to_add)
                is_header = False
            else:
                row = split_col(row)
                # bring to intermediate format
                row = add_blank_col_to_row(row, headers_to_add)
                lines.append(row)

    new_filename = 'sumstats_formatted_' + filename + '.tsv'
    with open(new_filename, 'w') as result_file:
        writer = csv.writer(result_file, delimiter='\t')
        writer.writerows([new_header])
        writer.writerows(lines)

    print("\n")
    print("------> Output saved in file:", new_filename, "<------")

if __name__ == "__main__":
    main()
