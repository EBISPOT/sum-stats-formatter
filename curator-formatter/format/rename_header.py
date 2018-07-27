import argparse
from format.read_perform_write import *


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-old', help='The original name of the header that will be renamed', required=True)
    argparser.add_argument('-new', help='The name of the header after it is renamed', required=True)
    args = argparser.parse_args()

    file = args.f
    header_old = args.old
    header_new = args.new

    def rename_header(header_old, header_new, header):
        if header_old in header and header_new not in header:
            index_old = header.index(header_old)
            header[index_old] = header_new
            return header
        else:
            raise ValueError("Old header not in the file OR new header already exists in file:", header_old,
                             header_new)

    open_close_perform(file=file, header_function=rename_header, args=dict(header_old=header_old, header_new=header_new))

    print("\n")
    print("------> Renamed data saved in:", file, "<------")


if __name__ == "__main__":
    main()