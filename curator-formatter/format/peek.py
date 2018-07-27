import argparse


def peek(file):

    with open(file) as open_file:
        header = open_file.readline().split()
        row = open_file.readline().split()

        for index, h in enumerate(header):
            print(h + " : " + row[index])


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    args = argparser.parse_args()

    file = args.f

    print("\n")
    print("------> Peeking into file:", file, "<------")

    peek(file)


if __name__ == "__main__":
    main()