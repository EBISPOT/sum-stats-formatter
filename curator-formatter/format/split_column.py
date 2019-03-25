import argparse
import glob
from tqdm import tqdm
import os
import pandas as pd
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
     
    df = pd.read_csv(file, comment='#', sep=sep, dtype=str, index_col=False, error_bad_lines=False, warn_bad_lines=True)
    header = df.columns.values

    if old_header in header:
        df = df.join(df[old_header].str.split(delimiter, expand=True).add_prefix(old_header).fillna('NA'))
        df[left_header] = df[old_header + '0'] 
        df[right_header] = df[old_header + '1']
    
        df.to_csv(new_filename, sep="\t", na_rep="NA", index=False)
        print("------> Split data saved in:", new_filename, "<------")

    else:
        raise ValueError("Couldn't find header: " , old_header)


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
