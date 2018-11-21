import argparse
import os
from format.utils import *
from tqdm import tqdm


def open_close_perform(file, left_header, right_header, delimiter, new_header):
    filename = get_filename(file)
    header = []
    mod_header = []
    is_header = True

    with open(file) as csv_file, open('.tmp.tsv', 'w') as result_file:
        csv_reader = get_csv_reader(csv_file)
        writer = csv.writer(result_file, delimiter='\t')
        row_count = get_row_count(file)

        for row in tqdm(csv_reader, total=row_count, unit="rows"):
            if is_header:
                header.extend(row)
                is_header = False
                mod_header.extend(row)
                mod_header = remove_from_row(row=mod_header, header=mod_header[:], left_header=left_header, right_header=right_header)
                mod_header.append(new_header)
                writer.writerows([mod_header])
            else:
                row = merge_columns(left_header=left_header, right_header=right_header, header=header[:], row=row, delimiter=delimiter)
                writer.writerows([row])

    os.rename('.tmp.tsv', filename + ".tsv")


def remove_from_row(row, header, left_header, right_header):
    row.pop(header.index(left_header))
    header.pop(header.index(left_header))
    row.pop(header.index(right_header))
    return row


def merge_columns(left_header, right_header, header, row, delimiter):
    column_1 = row[header.index(left_header)]
    column_2 = row[header.index(right_header)]
    row = remove_from_row(row=row, header=header, right_header=right_header, left_header=left_header)
    row.append(column_1 + delimiter + column_2)
    return row


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-left', help='The header of the first column that will be merged', required=True)
    argparser.add_argument('-right', help='The header of the second column that will be merged', required=True)
    argparser.add_argument('-d', help='The delimiter that the columns will be united with', required=True)
    argparser.add_argument('-new', help='The header of the new merged column', required=True)
    args = argparser.parse_args()

    file = args.f
    left_header = args.left
    right_header = args.right
    delimiter = args.d
    new_header = args.new

    open_close_perform(file=file, delimiter=delimiter, left_header=left_header, right_header=right_header, new_header=new_header)

    print("\n")
    print("------> Merged data saved in:", file, "<------")


if __name__ == "__main__":
    main()
