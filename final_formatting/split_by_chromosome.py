import csv
import argparse
import os


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The full path to the file to be processed', required=True)
    argparser.add_argument('-chr', help='Chromosome whose data we are going to extract', required=True)
    args = argparser.parse_args()

    file = args.f
    path = os.path.dirname(os.path.abspath(__file__))
    chromosome = int(args.chr)
    filename = file.split("/")[-1].split(".")[0]

    chromosome_index = None
    is_header = True
    total_of_lines = 0

    new_filename = os.path.join(path, 'chr_' + str(chromosome) + "_" + filename + '.tsv')

    result_file = open(new_filename, 'w')
    writer = csv.writer(result_file, delimiter='\t')
    with open(file) as csv_file:
        dialect = csv.Sniffer().sniff(csv_file.readline())
        csv_file.seek(0)
        csv_reader = csv.reader(csv_file, dialect)
        for row in csv_reader:
            if is_header:
                header = row
                chromosome_index = header.index('chromosome')
                writer.writerows([header])
                is_header = False
            else:
                if int(row[chromosome_index]) == chromosome:
                    writer.writerows([row])
                    total_of_lines += 1

    result_file.close()

    if total_of_lines == 0:
        # nothing was written, delete the file
        os.remove(new_filename)


if __name__ == "__main__":
    main()
