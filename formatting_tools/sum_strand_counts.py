import csv
import sys
import glob
import argparse


def aggregate_counts(in_dir, out_dir): 

    variant_type_dict = {
        "Palindormic variant": 0,
        "Forward strand variant": 0,
        "Reverse strand variant": 0,
        "No VCF record found": 0,
        "Invalid variant for harmonisation": 0
    }

    # decisions based on counts:
    test_dict = {
        "forward": variant_type_dict["Forward strand variant"] > 0 and variant_type_dict["Reverse strand variant"] == 0,
        "reverse": variant_type_dict["Reverse strand variant"] > 0 and variant_type_dict["Forward strand variant"] == 0,
        "mixed": variant_type_dict["Forward strand variant"] > 0 and variant_type_dict["Reverse strand variant"] > 0,
        "no_consensus": variant_type_dict["Forward strand variant"] == 0 and variant_type_dict["Reverse strand variant"] == 0
    }
    all_files = glob.glob(in_dir + "strand_count_*.tsv")

    with open(out_dir + "total_strand_count.csv", 'w') as tsvout:
        for f in all_files:
            with open(f,'r') as tsvin:
                tsvin = csv.reader(tsvin, delimiter='\t')
                for line in tsvin:
                    for variant_type, count in variant_type_dict.items():
                        if variant_type == line[0]:
                            variant_type_dict[variant_type] += int(line[1])
 
        for variant_type, count in variant_type_dict.items():
            tsvout.write(variant_type + '\t' + str(count) + '\n')

        for mode, test in test_dict.items():
            if test is True:
                tsvout.write("palin_mode" + "\t" + mode.replace("mixed", "drop").replace("no_consensus", "drop"))

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-i', help='The directory of strand ccount files', required=True)
    argparser.add_argument('-o', help='The directory to write to', required=True)
    args = argparser.parse_args()
    
    in_dir = args.i
    out_dir = args.o
    aggregate_counts(in_dir, out_dir)


if __name__ == "__main__":
    main()
