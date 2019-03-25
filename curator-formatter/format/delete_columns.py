import argparse
import glob
from tqdm import tqdm
import os
import pandas as pd
from format.utils import *


def process_file(file, headers):
    filename, file_extension = os.path.splitext(file)
    new_filename = 'del_' + filename + '.tsv'

    sep = '\s+'
    if file_extension == '.csv':
        sep = ','
     
    df = pd.read_csv(file, comment='#', sep=sep, dtype=str, index_col=False, error_bad_lines=False, warn_bad_lines=True)
    df = df.drop(columns=headers)
    
    df.to_csv(new_filename, sep="\t", na_rep="NA", index=False)


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
