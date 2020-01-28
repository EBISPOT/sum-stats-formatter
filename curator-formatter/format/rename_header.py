import pandas as pd
import os
import argparse
import glob


def process_file(file, header_old, header_new):
    mapper = {header_old: header_new}
    filename, file_extension = os.path.splitext(file)
    new_filename = 'renamed_' + os.path.basename(filename) + '.tsv'
    sep = '\s+'
    if file_extension == '.csv':
        sep = ','
    
    df = pd.read_csv(file, comment='#', sep=sep, dtype=str, index_col=False, error_bad_lines=False, warn_bad_lines=True, chunksize=1000000)
    first = True
    for chunk in df:
        chunk.rename(columns = mapper, inplace = True)
        if first:
            chunk.to_csv(new_filename, mode='w', header=True, sep="\t", na_rep="NA", index=False)
            first = False
        else:
            chunk.to_csv(new_filename, mode='a', header=False, sep="\t", na_rep="NA", index=False)

    print("\n")
    print("------> Renamed data saved in:", new_filename, "<------")
    

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed')
    argparser.add_argument('-dir', help='The name of the directory containing the files that need to processed')
    argparser.add_argument('-old', help='The original name of the header that will be renamed', required=True)
    argparser.add_argument('-new', help='The name of the header after it is renamed', required=True)
    args = argparser.parse_args()

    header_old = args.old
    header_new = args.new

    if args.f and args.dir is None:
        file = args.f
        process_file(file, header_old, header_new)
    elif args.dir and args.f is None:
        dir = args.dir
        print("Processing the following files:")
        for f in glob.glob("{}/*".format(dir)):
            print(f)
            process_file(f, header_old, header_new)
    else:
        print("You must specify either -f <file> OR -dir <directory containing files>")


if __name__ == "__main__":
    main()
