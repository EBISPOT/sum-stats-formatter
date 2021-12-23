import argparse
import os
from format.helpers.utils import *


def get_value_from_field(row, header, field):
    col_index = header.index(field)
    return row[col_index], col_index


def str_to_bool(string):
    if string.lower() == 'true':
        return True
    elif string.lower() == 'false':
        return False
    else:
        raise ValueError("Cannot covert {} to a bool".format(string))


def swap_alleles(row, header, h):
    keep_alleles = str_to_bool(get_value_from_field(row, header, h)[0])
    effect_allele, effect_allele_index = get_value_from_field(row, header, EFFECT_ALLELE)
    other_allele, other_allele_index = get_value_from_field(row, header, OTHER_ALLELE)
    if keep_alleles is False:
        row[effect_allele_index], row[other_allele_index] = other_allele, effect_allele
    return row


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-header', help='The header of the column with the TRUE/FALSE values', required=True)
    args = argparser.parse_args()

    file = args.f
    h = args.header
    filename = get_filename(file)
    is_header = True
    header = None
    lines = []

    with open(file) as csv_file:
        csv_reader = get_csv_reader(csv_file)
        for row in csv_reader:
            if is_header:
                is_header = False
                header = row
            else:
                row = swap_alleles(row, header, h)
                lines.append(row)

    with open('.tmp.tsv', 'w') as result_file:
        writer = csv.writer(result_file, delimiter='\t')
        writer.writerows([header])
        writer.writerows(lines)

    os.rename('.tmp.tsv', filename + ".tsv")

    print("\n")
    print("------> Processed data saved in:", file, "<------")

if __name__ == "__main__":
    main()