import argparse
import pandas as pd
import os
from tabulate import tabulate


def peek(file, num_lines=10, sep='\s+', ignore='#'):
    df = pd.read_csv(file, sep=sep, dtype=str, 
                    comment=ignore, error_bad_lines=False, 
                    warn_bad_lines=True, nrows=num_lines,
                    na_filter=False)
    return tabulate(df, headers='keys', tablefmt="fancy_grid", disable_numparse=True, showindex=False)


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-n', help='Number of lines to display (default=10)', default=10, type=int, required=False)
    args = argparser.parse_args()
    file = args.f
    num_lines = args.n
    print("\n")
    print("------> Peeking into file:", file, "<------\n")
    head = peek(file, num_lines)
    print(head)


if __name__ == "__main__":
    main()
