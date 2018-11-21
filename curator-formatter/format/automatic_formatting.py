import csv
import argparse
from tqdm import tqdm

from format import peek
from format.utils import *


def cleanup_chromosome(chromosome):
    if 'chr' in chromosome:
        chromosome = chromosome.replace('chr', '')
    if 'CHR' in chromosome:
        chromosome = chromosome.replace('CHR', '')
    if '_' in chromosome:
        chromosome = chromosome.replace('_', '')
    if '-' in chromosome:
        chromosome = chromosome.replace('-', '')
    return chromosome


def can_split_chr_pos(item):
    return ':' in item or '_' in item


def split_chr_pos(item):
    if ':' in item:
        chromosome = item.split(":")[0]
        bp = item.split(":")[1]
    elif "_" in item:
        chromosome = item.split("_")[0]
        bp = item.split("_")[1]
    else:
        raise RuntimeError("Do not know how chromosome and base pair location are separated!")

    chromosome = cleanup_chromosome(chromosome)

    return str(chromosome), str(bp)


def add_info_to_header(header):
    if CHR_BP in header:
        header.pop(header.index(CHR_BP))
    header.append(CHR)
    header.append(BP)
    return header


def process_row(row, header):
    if CHR_BP in header:
        # split up into chromosome and position
        index_chrpos = header.index(CHR_BP)
        chromosome, position = split_chr_pos(row[index_chrpos])
        row.pop(index_chrpos)
        row.append(chromosome)
        row.append(position)
    elif CHR in header:
        index_chrpos = header.index(CHR)
        chromosome = cleanup_chromosome(row[index_chrpos])
        row[index_chrpos] = chromosome
    elif CHR not in header and BP not in header:
        index_snp = header.index(VARIANT)
        if can_split_chr_pos(row[index_snp]):
            chromosome, position = split_chr_pos(row[index_snp])
            row.append(chromosome)
            row.append(position)
    return row


def multi_delimiters_to_single(row):
    return "\t".join(row.split())



def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    args = argparser.parse_args()

    file = args.f
    filename = get_filename(file)
    what_changed = None
    new_header = None
    extended_header = None
    is_header = True
    first_line = True

    new_filename = 'formatted_' + filename + '.tsv'
    with open(file) as csv_file, open(new_filename, 'w') as result_file, open(file, 'r') as f:
        csv_reader = get_csv_reader(csv_file)
        writer = csv.writer(result_file, delimiter='\t')
        row_count = len(f.readlines())
        for row in tqdm(csv_reader, total=row_count, unit="rows"):
            row = multi_delimiters_to_single('\t'.join(row)).split()
            if is_header:
                what_changed = mapped_headers(row[:])
                new_header = refactor_header(row)
                is_header = False
            elif first_line:
            # if we have added chromosome and base pair location to the end of the file
            # we also need to add it to the header
                row = process_row(row, new_header)
                if len(new_header) + 1 == len(row) or len(new_header) + 2 == len(row):
                    extended_header = add_info_to_header(new_header)
                    writer.writerows([extended_header])
                else:
                    writer.writerows([new_header])
                writer.writerows([row])
                first_line = False
            else:
                row = process_row(row, new_header)
                writer.writerows([row])


    print("\n")
    print("------> Output saved in file:", new_filename, "<------")
    print("\n")
    print("Please use this file for any further formatting.")
    print("\n")
    print("Showing how the headers where mapped below...")
    print("\n")
    for key, value in what_changed.items():
        print(key, " -> ", value)
    print("\n")
    print("Peeking into the new file...")
    print("\n")
    peek.peek(new_filename)


if __name__ == "__main__":
    main()
