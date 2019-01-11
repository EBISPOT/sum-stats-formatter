import argparse
import glob
from tqdm import tqdm
import os
from format.utils import *


def header_index(header, h):
    if h not in header:
        raise ValueError("Header specified is not in the file:", h)
    return header.index(h)


def open_close_perform(file, delimiter, old_header, left_header, right_header):
    filename = get_filename(file)
    header = []
    mod_header = []
    index_h = None
    is_header = True
    with open(file) as csv_file, open('.tmp.tsv', 'w') as result_file:
        csv_reader = get_csv_reader(csv_file)
        writer = csv.writer(result_file, delimiter='\t')
        row_count = get_row_count(file)

        for row in tqdm(csv_reader, total=row_count, unit="rows"):
            if is_header:
                is_header = False
                header.extend(row)
                mod_header.extend(row)
                index_h = header_index(header=header, h=old_header)
                mod_header.pop(index_h)
                mod_header.append(left_header)
                mod_header.append(right_header)
                writer.writerows([mod_header])
            else:
                row = split_columns(index_h=index_h, row=row, delimiter=delimiter)
                writer.writerows([row])

    os.rename('.tmp.tsv', filename + ".tsv")


def split_columns(index_h, row, delimiter):
    column = row[index_h]
    if delimiter not in column:
        raise ValueError("Wrong delimiter:" , delimiter)
    row.pop(index_h)
    c1 = column.split(delimiter)[0]
    c2 = column.split(delimiter)[1]
    row.append(c1)
    row.append(c2)
    return row


def process_file(file, header, left_header, right_header, delimiter):
    open_close_perform(file=file, old_header=header, left_header=left_header, right_header=right_header, delimiter=delimiter)

    print("\n")
    print("------> Split data saved in:", os.path.basename(file), "<------")


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed')
    argparser.add_argument('-dir', help='The name of the directory containing the files that need to processed')
    argparser.add_argument('-header', help='The header of the column that will be split', required=True)
    argparser.add_argument('-left', help='The new header 2 (will take the left part after the split)', required=True)
    argparser.add_argument('-right', help='The new header 1 (will take the right part after the split)', required=True)
    argparser.add_argument('-d', help='The delimiter that the column will be separated by', required=True)
    args = argparser.parse_args()

    header = args.header
    right_header = args.right
    left_header = args.left
    delimiter = args.d

    if args.f and args.dir is None:
        file = args.f
        process_file(file, header, left_header, right_header, delimiter)
    elif args.dir and args.f is None:
        dir = args.dir
        print("Processing the following files:")
        for f in glob.glob("{}/*".format(dir)):
            print(f)
            process_file(f, header, left_header, right_header, delimiter)
    else:
        print("You must specify either -f <file> OR -dir <directory containing files>")


if __name__ == "__main__":
    main()
