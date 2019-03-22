import glob
import argparse
from tqdm import tqdm
import pandas as pd
from format import peek
from format.utils import *


def multi_delimiters_to_single(row):
    return "\t".join(row.split())


def process_file(file):
    filename = get_filename(file)

    new_filename = 'formatted_' + filename + '.tsv'

    tqdm.pandas()
    df = pd.read_csv(file, low_memory=True, index_col=False, sep="\s+", dtype=str, error_bad_lines=False, warn_bad_lines=True)

    # map headers
    header = df.columns.values
    df.rename(columns=known_header_transformations, inplace=True)
    new_header = df.columns.values
    what_changed = dict(zip(header, new_header))

    if CHR_BP in new_header:
        # split the chr_bp field
        df = df.join(df[CHR_BP].str.split('_|:', expand=True).add_prefix(CHR_BP).fillna('NA'))
        df[CHR] = df[CHR_BP + '0'].str.replace('CHR|chr|_|-', '', regex=True)
        df[BP] = df[VARIANT + '1']

    elif CHR in new_header:
        # clean the chr field
        df[CHR] = df[CHR].str.replace('CHR|chr|_|-', '', regex=True)

    elif CHR not in new_header and BP not in new_header:
        # split the snp field
        df = df.join(df[VARIANT].str.split('_|:', expand=True).add_prefix(VARIANT).fillna('NA'))
        df[CHR] = df[VARIANT + '0'].str.replace('CHR|chr|_|-|rs.*|ss.*', '', regex=True).replace('', 'NA', regex=True)
        df[BP] = df[VARIANT + '1']

    df.to_csv(new_filename, sep="\t", na_rep="NA", index=False)

    print("\n")
    print("------> Output saved in file:", new_filename, "<------")
    print("\n")
    print("Please use this file for any further formatting.")
    print("\n")
    print("Showing how the headers where mapped below...")
    print("\n")
    for key, value in what_changed.items():
        print(key, " -> ", value)
    print("\n")
    print("Peeking into the new file...")
    print("\n")
    peek.peek(new_filename)


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed')
    argparser.add_argument('-dir', help='The name of the directory containing the files that need to processed')
    args = argparser.parse_args()

    if args.f and args.dir is None:
        file = args.f
        process_file(file)
    elif args.dir and args.f is None:
        dir = args.dir
        print("Processing the following files:")
        for f in glob.glob("{}/*".format(dir)):
            print(f)
            process_file(f)
    else:
        print("You must specify either -f <file> OR -dir <directory containing files>")


if __name__ == "__main__":
    main()
