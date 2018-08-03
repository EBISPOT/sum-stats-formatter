import csv
import sys
import argparse

sys_paths = ['sumstats/','../sumstats/','../../sumstats/']
sys.path.extend(sys_paths)
from sumstats_formatting import *
from common_constants import *


# 1) must have SNP, PVAL, CHR, BP
# 2) remove rows with blank values for (1)
# 3) conform to data types:
#   - if pval not floats: remove row
#   - if chr and bp not ints: remove row


REQUIRED_HEADERS = [SNP_DSET, PVAL_DSET, CHR_DSET, BP_DSET]
BLANK_SET = {'', ' ', '-', '.'}

def check_for_required_headers(header):
    return list(set(REQUIRED_HEADERS) - set(header))


def get_header_indices(header):
    return [header.index(h) for h in REQUIRED_HEADERS if h in header] 


def required_elements(row, header):
    return [row[i] for i in get_header_indices(header)]
      

def remove_row_if_required_is_blank(row, header):
    blanks = BLANK_SET & set(required_elements(row, header))
    if blanks:
        return True
    else:
        return False


def blanks_to_NA(row):
    for n, i in enumerate(row):
        if i in BLANK_SET:
            row[n] = 'NA'
    return row
            

def remove_row_if_wrong_data_type(row, header, col, data_type):
    try:
        data_type(row[header.index(col)])
        return False
    except ValueError:
        return True


def map_x_y_to_23_24(row, header):
    index_chr = header.index(CHR_DSET)
    chromosome = row[index_chr].lower()
    if 'x' in chromosome:
        chromosome = '23'
    if 'y' in chromosome:
        chromosome = '24'
    row[index_chr] = chromosome
    return row


def drop_last_element_from_filename(filename):
    filename_parts = filename.split('-')
    return '-'.join(filename_parts[:-1])


def main():

    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-d', help='The name of the output directory', required=True)
    argparser.add_argument('--print_only', help='only print the lines removed and do not write a new file', action='store_true')
    args = argparser.parse_args()

    file = args.f
    out_dir = args.d
    filename = get_filename(file)

    new_filename = out_dir + drop_last_element_from_filename(filename) + '.tsv' # drop the build from the filename
    header = None
    is_header = True
    lines = []
    removed_lines =[]
    missing_headers = []

    with open(file) as csv_file:
        result_file = None
        writer = None
        if not args.print_only:
            result_file = open(new_filename, 'w')
            writer = csv.writer(result_file, delimiter='\t')
        csv_reader = get_csv_reader(csv_file)

        for row in csv_reader:
            if is_header:
                header = row
                is_header = False
                missing_headers = check_for_required_headers(header)
                if missing_headers:
                    sys.exit("Required headers are missing!:{}".format(missing_headers))
                if not args.print_only:
                    writer.writerows([header])
            else:
                # Checks for blanks, integers and floats:
                row = blanks_to_NA(row)
                row = map_x_y_to_23_24(row, header)
                blank = remove_row_if_required_is_blank(row, header)
                wrong_type_chr = (remove_row_if_wrong_data_type(row, header, CHR_DSET, int)) # can get this from common_constants.py if h5py installed
                wrong_type_bp = (remove_row_if_wrong_data_type(row, header, BP_DSET, int))
                wrong_type_pval = (remove_row_if_wrong_data_type(row, header, PVAL_DSET, float))
                remove_row_tests = [blank == False,
                                    wrong_type_chr == False,
                                    wrong_type_bp == False,
                                    wrong_type_pval == False]
                if all(remove_row_tests):
                    if not args.print_only:
                        writer.writerows([row])
                else:
                #    removed_lines.append(row)
                    print(row)

    #print("kept: {} rows".format(len(lines)))
    #print("removed: {} rows".format(len(removed_lines)))

    #with open(new_filename, 'w') as result_file:
    #    writer = csv.writer(result_file, delimiter='\t')
    #    writer.writerows([header])
    #    writer.writerows(lines)
           
if __name__ == "__main__":
    main()
