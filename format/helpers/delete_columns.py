import argparse
import glob
from tqdm import tqdm
import os
import dask.dataframe as dd
import pandas as pd
from format.helpers.utils import *

def process_file(file, drop_headers):
    filename, file_extension = os.path.splitext(file)
    new_filename = 'del_' + os.path.basename(filename) + '.tsv'
    sep = '\s+'
    if file_extension == '.csv':
        sep = ','
    header =  pd.read_csv(file, comment='#', sep=sep, index_col=0, nrows=0).columns.tolist()
    header = [h for h in header if h not in drop_headers]
    df = dd.read_csv(file, comment='#', sep=sep, usecols=header, dtype=str, error_bad_lines=False, warn_bad_lines=True)
    df.to_csv(new_filename, mode='w', header=True, sep="\t", na_rep="NA", index=False, single_file=True)
    print("\n")
    print("------> Processed data saved in:", new_filename, "<------")


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed')
    argparser.add_argument('-dir', help='The name of the directory containing the files that need to processed')
    argparser.add_argument('-headers', help='Header(s) that you want removed. If more than one, enter comma-separated, with no spaces', required=True)
    args = argparser.parse_args()

    headers = list(args.headers.split(","))

    if args.f and args.dir is None:
        file = args.f
        if os.path.exists(file):
            process_file(file, headers)
    elif args.dir and args.f is None:
        dir = args.dir
        print("Processing the following files:")
        for f in glob.glob("{}/*".format(dir)):
            print(f)
            process_file(f, headers)
    else:
        print("You must specify either a single file with `-f <file>` OR many files with `-dir <directory containing files>`")


if __name__ == "__main__":
    main()
