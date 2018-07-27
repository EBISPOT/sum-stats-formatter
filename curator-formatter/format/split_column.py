import argparse
import os
from format.utils import *


def header_index(header, h):
    if h not in header:
        raise ValueError("Header specified is not in the file:", h)
    return header.index(h)


def open_close_perform(file, delimiter, old_header, left_header, right_header):
    filename = get_filename(file)
    header = None
    index_h = None
    is_header = True
    lines = []
    with open(file) as csv_file:
        csv_reader = get_csv_reader(csv_file)
        for row in csv_reader:
            if is_header:
                is_header = False
                header = row
                index_h = header_index(header=header, h=old_header)
            else:
                row = split_columns(index_h=index_h, row=row, delimiter=delimiter)
                lines.append(row)

    header.pop(index_h)
    header.append(left_header)
    header.append(right_header)
    with open('.tmp.tsv', 'w') as result_file:
        writer = csv.writer(result_file, delimiter='\t')
        writer.writerows([header])
        writer.writerows(lines)

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


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-header', help='The header of the column that will be split', required=True)
    argparser.add_argument('-left', help='The new header 2 (will take the left part after the split)', required=True)
    argparser.add_argument('-right', help='The new header 1 (will take the right part after the split)', required=True)
    argparser.add_argument('-d', help='The delimiter that the column will be separated by', required=True)
    args = argparser.parse_args()

    file = args.f
    header = args.header
    right_header = args.right
    left_header = args.left
    delimiter = args.d

    open_close_perform(file=file, old_header=header, left_header=left_header, right_header=right_header, delimiter=delimiter)

    print("\n")
    print("------> Split data saved in:", file, "<------")


if __name__ == "__main__":
    main()