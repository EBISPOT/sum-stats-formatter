import argparse
import glob
from format.read_perform_write import *


def rename_header(header_old, header_new, header):
    if header_old in header and header_new not in header:
        index_old = header.index(header_old)
        header[index_old] = header_new
        return header
    else:
        raise ValueError("Old header not in the file OR new header already exists in file:", header_old,
                         header_new)

def process_file(file, header_old, header_new):
    open_close_perform(file=file, header_function=rename_header, args=dict(header_old=header_old, header_new=header_new))

    print("\n")
    print("------> Renamed data saved in:", os.path.basename(file), "<------")
    

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
