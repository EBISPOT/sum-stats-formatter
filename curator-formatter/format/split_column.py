import argparse
import glob
from tqdm import tqdm
import os
import pandas as pd
import dask.dataframe as dd
from format.utils import *


def header_index(header, h):
    if h not in header:
        raise ValueError("Header specified is not in the file:", h)
    return header.index(h)


def process_file(file, old_header, left_header, right_header, delimiter):
    filename, file_extension = os.path.splitext(file)
    new_filename = 'split_' + filename + '.tsv'

    sep = '\s+'
    if file_extension == '.csv':
        sep = ','
     
    df = dd.read_csv(file, comment='#', sep=sep, dtype=str, error_bad_lines=False, warn_bad_lines=True)
    df = split_field(df, old_header, delimiter, left_header, right_header)
    df = df.replace('', 'NA')
    df.to_csv(new_filename, mode='w', header=True, sep="\t", na_rep="NA", index=False, single_file=True)

    print("------> Split data saved in:", new_filename, "<------")


def split_field(df, field, delimiter, left_name, right_name):
    newdf = df.join(df[field].str.partition(delimiter, expand=True))
    if 1 in newdf and 2 in newdf:
        df = newdf.rename(columns={0:left_name, 2:right_name}).drop(columns=[1])
    else:
        print("could not perform split")
    return df



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
