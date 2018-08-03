import argparse
import csv
import os


wd = os.path.dirname(os.path.abspath(__file__))
max_bp = 30000000
bp_step = 16
bp_range = int(max_bp / bp_step)


def make_harmonise_config(filename, step, chromosome, bp_start, bp_end):
    out_name = 'run_bp_{step}.{ss_name}.sh'.format(step=step, ss_name=filename)
    with open('config_temp.sh', 'r') as temp:
        with open(out_name, 'w') as out:
            for line in temp:
                out.write(line.format(ss_name=filename, chromosome=chromosome, step=step, bp_start=bp_start, bp_end=bp_end))


def read_process_write(file, chromosome, filename, path, accession, start, end):
    bp_index = None
    is_header = True
    total_of_lines = 0
    new_filename = os.path.join(path, 'bp_' + accession + "." + filename + '.tsv')
    result_file = open(new_filename, 'w')
    writer = csv.writer(result_file, delimiter='\t')

    with open(file) as csv_file:
        dialect = csv.Sniffer().sniff(csv_file.readline())
        csv_file.seek(0)
        csv_reader = csv.reader(csv_file, dialect)
        for row in csv_reader:
            if is_header:
                header = row
                bp_index = header.index('base_pair_location')
                is_header = False
                writer.writerows([header])
            else:
                if start <= int(row[bp_index]) < end:
                    writer.writerows([row])
                    total_of_lines += 1
    result_file.close()
    make_harmonise_config(filename, accession, chromosome, start, end)


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The full path to the file to be processed', required=True)
    argparser.add_argument('-chr', help='chromosome', required=True)
    args = argparser.parse_args()

    file = args.f
    chromosome = args.chr
    path = wd
    filename = os.path.splitext(file)[0]

    previous = 0
    step = 1
    for bp in range(0, max_bp, bp_range):
        print("bp: {}".format(step))
        
        read_process_write(file, chromosome=chromosome, filename=filename, path=path, accession=str(step), start=previous, end=bp)
        previous = bp
        step += 1


    #if total_of_lines == 0:
    #    # nothing was written, delete the file
    #    os.remove(new_filename)


if __name__ == "__main__":
    main()
