import argparse
import glob
from tqdm import tqdm
import os
from format.utils import *


def open_close_perform(file, headers):
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
                is_header = False
                header.extend(row)
                mod_header.extend(row)
                for h in headers:
                    mod_header = remove_from_row(row=mod_header, header=mod_header[:], column=h)
                writer.writerows([mod_header])
            else:
                for h in headers:
                    row = remove_from_row(row=row, header=header[:], column=h)
                writer.writerows([row])


    os.rename('.tmp.tsv', filename + ".tsv")


def remove_from_row(row, header, column):
    row.pop(header.index(column))
    return row


def process_file(file, headers):
    open_close_perform(file=file, headers=headers)

    print("\n")
    print("------> Processed data saved in:", os.path.basename(file), "<------")


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed')
    argparser.add_argument('-dir', help='The name of the directory containing the files that need to processed')
    argparser.add_argument('-headers', help='Header(s) that you want removed. If more than one, enter comma-separated', required=True)
    args = argparser.parse_args()

    headers = args.headers.split(",")

    if args.f and args.dir is None:
        file = args.f
        process_file(file, headers)
    elif args.dir and args.f is None:
        dir = args.dir
        print("Processing the following files:")
        for f in glob.glob("{}/*".format(dir)):
            print(f)
            process_file(f, headers)
    else:
        print("You must specify either -f <file> OR -dir <directory containing files>")


if __name__ == "__main__":
    main()
