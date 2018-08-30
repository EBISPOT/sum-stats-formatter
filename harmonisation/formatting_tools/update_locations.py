import argparse
import csv
import os
import subprocess
import logging

from utils import *
from liftover import *


#CHROMOSOMES = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', 'X', 'Y']
CHROMOSOMES = get_chromosome_list()

logger = logging.getLogger('update_locations')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')


def all_same(items):
    return all(x == items[0] for x in items)


def select_canonical_data(mapped_data):
    chrom = []
    bp = []
    for line in mapped_data:
        if line.split('\t')[3] in CHROMOSOMES:
            bp.append(line.split('\t')[5])
            chrom.append(line.split('\t')[3])
    if len(bp) == 1:
        return  chrom[0], bp[0]
    elif len(bp) > 1 and all_same(bp):
        return chrom[0], bp[0] #"AMBIGUOUS"
    else:
        return "NA", "NA" # to catch those where they only map to a patch


def open_process_file(file, out_dir, from_build, to_build):
    filename = file.split("/")[-1].split(".")[0]
    path = os.path.dirname(file)

    with open(file, 'r') as in_file, open('{path}/{filename}.tsv.out'.format(path = path, filename = filename), 'r') as ensembl_file:
        result_file = open('{out_dir}/{filename}.tsv'.format(out_dir = out_dir, filename = filename), 'w')
        csv_reader = csv.DictReader(in_file, delimiter='\t')
        fieldnames = csv_reader.fieldnames
        writer = csv.DictWriter(result_file, fieldnames=fieldnames, delimiter='\t')
        writer.writeheader()
        ensembl_data = set(ensembl_file.read().splitlines())
        build_map = None
        if from_build != to_build:
            build_map = LiftOver(ucsc_release.get(from_build), ucsc_release.get(to_build))
        for row in csv_reader:
            variant_id = row[SNP_DSET]
            chromosome = row[CHR_DSET].replace('23', 'X').replace('24', 'Y').replace('25', 'MT')
            bp = row[BP_DSET]
            variant_string = variant_id + '\t'
            mapped_data = [line for line in ensembl_data if variant_string in line]

            if mapped_data:
                row[CHR_DSET], row[BP_DSET] = select_canonical_data(mapped_data)
                logger.info('Mapping {0}:{1}:{2} to {3}:{4}'.format(variant_id, chromosome, bp, row[CHR_DSET], row[BP_DSET]))
            # do the bp location mapping if needed
            elif from_build != to_build:
                mapped_bp = map_bp_to_build_via_liftover(chromosome=chromosome, bp=bp, build_map=build_map)
                logger.info('liftover {0}:{1}:{2} from build {3} to {4}'.format(variant_id, chromosome, bp, from_build, mapped_bp))
                if mapped_bp is None:
                    mapped_bp = map_bp_to_build_via_ensembl(chromosome=chromosome, bp=bp, from_build=from_build, to_build=to_build)
                    logger.info('Ensembl REST API mapping {0}:{1}:{2} from build {3} to {4}'.format(variant_id, chromosome, bp, from_build, mapped_bp))
                row[BP_DSET] = mapped_bp
            else:
                row[BP_DSET] = "NA" #"UNMAPPED"
                logger.info('Unable ot map {0} to build {1}'.format(variant_id, to_build))
            print(row)
            writer.writerow(row)
        
            
def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The full path to the file to be processed', required=True)
    argparser.add_argument('-d', help='The directory to write to', required=True)
    argparser.add_argument('-from_build', help='The original build e.g. "36" for NCBI36 or hg18', required=True)
    argparser.add_argument('-to_build', help='The latest (desired) build e.g. "38"', required=True)
    argparser.add_argument('--log', help='The name of the log file')
    args = argparser.parse_args()
    
    file = args.f
    out_dir = args.d
    from_build = args.from_build
    to_build = args.to_build
    log_file = args.log

    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    open_process_file(file, out_dir, from_build, to_build)


if __name__ == "__main__":
    main()
