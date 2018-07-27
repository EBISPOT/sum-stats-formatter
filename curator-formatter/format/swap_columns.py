import argparse
from format.read_perform_write import *


def swap_headers(left_header, right_header, header):
    if left_header and right_header in header:
        index_h1 = header.index(left_header)
        index_h2 = header.index(right_header)
        header[index_h1] = right_header
        header[index_h2] = left_header
        return header
    else:
        raise ValueError("Headers specified are not in the file:", left_header, right_header)


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-left', help='The first header that will be swapped', required=True)
    argparser.add_argument('-right', help='The second header that will be swapped with the first one', required=True)
    args = argparser.parse_args()

    file = args.f
    left_header = args.left
    right_header = args.right

    open_close_perform(file=file, header_function=swap_headers, args=dict(left_header=left_header, right_header=right_header))

    print("\n")
    print("------> Swapped data saved in:", file, "<------")


if __name__ == "__main__":
    main()