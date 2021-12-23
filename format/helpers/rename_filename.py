import os
import argparse


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-efo', help='The EFO trait that is related to this file', required=True)
    argparser.add_argument('-study', help='The study accession that is related to this file', required=True)
    argparser.add_argument('-b', help='The genome build that is related to this file', required=True)
    argparser.add_argument('-pmid', help='The pubmed id that is related to this file', required=True)
    argparser.add_argument('-author', help='The author that is related to this file', required=True)
    args = argparser.parse_args()

    file = args.f
    efo = args.efo
    study = args.study
    build = args.b
    pmid = args.pmid
    author = args.author

    new_name = author + "_" + pmid + "_" + study + "_" + efo + "_" + build + ".tsv"
    os.rename(file, new_name)

    print("\n")
    print("------> File renamed as:", new_name, "<------")


if __name__ == "__main__":
    main()
