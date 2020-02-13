import argparse
import os
from format.utils import *
from tqdm import tqdm



def header_index(header, h):
    if h not in header:
        raise ValueError("Header specified is not in the file:", h)
    return header.index(h)


def open_close_perform(file, h, fixture):
    filename = get_filename(file)
    header = None
    index_h = None
    is_header = True

    with open(file) as csv_file, open('.tmp.tsv', 'w') as result_file:
        csv_reader = get_csv_reader(csv_file)
        writer = csv.writer(result_file, delimiter='\t')
        row_count = get_row_count(file)
        
        
        for row in tqdm(csv_reader, total=row_count, unit="rows"):
            if is_header:
                is_header = False
                header = row
                index_h = header_index(header=header, h=h)
                writer.writerows([header])

            else:
                row = clean_row(index_h=index_h, row=row, fixture=fixture)
                writer.writerows([row])

    os.rename('.tmp.tsv', filename + ".tsv")


def clean_row(index_h, row, fixture):
    if fixture not in row[index_h]:
        raise ValueError("The fixture provided does not exist:", fixture)
    row[index_h] = row[index_h].strip(fixture)
    return row


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-header', help='The header of the column that will be cleaned', required=True)
    argparser.add_argument('-fixture', help='The sequence that will be removed from the column data', required=True)
    args = argparser.parse_args()

    file = args.f
    h = args.header
    fixture = args.fixture

    open_close_perform(file=file, h=h, fixture=fixture)

    print("\n")
    print("------> Cleaned data saved in:", file, "<------")


if __name__ == "__main__":
    main()
