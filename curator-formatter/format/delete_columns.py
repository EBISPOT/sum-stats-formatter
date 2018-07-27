import argparse
import os
from format.utils import *


def open_close_perform(file, headers):
    filename = get_filename(file)
    header = None
    is_header = True
    lines = []
    with open(file) as csv_file:
        csv_reader = get_csv_reader(csv_file)
        for row in csv_reader:
            if is_header:
                is_header = False
                header = row
            else:
                for h in headers:
                    row = remove_from_row(row=row, header=header[:], column=h)
                lines.append(row)

    for h in headers:
        header = remove_from_row(row=header, header=header[:], column=h)
    with open('.tmp.tsv', 'w') as result_file:
        writer = csv.writer(result_file, delimiter='\t')
        writer.writerows([header])
        writer.writerows(lines)

    os.rename('.tmp.tsv', filename + ".tsv")


def remove_from_row(row, header, column):
    row.pop(header.index(column))
    return row


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-headers', help='Header(s) that you want removed. If more than one, enter comma-separated', required=True)
    args = argparser.parse_args()

    file = args.f
    headers = args.headers.split(",")

    open_close_perform(file=file, headers=headers)

    print("\n")
    print("------> Processed data saved in:", file, "<------")


if __name__ == "__main__":
    main()