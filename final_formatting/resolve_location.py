import csv
import argparse

from utils import *
from ensembl_rest_client import EnsemblRestClient
from dbsnp_client import dbSNPclient


accepted_chr_values = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10',
                       '11', '12', '13', '14', '15', '16', '17', '18', '19',
                       '20', '21', '22', 'X', 'Y']

def parse_location_data(location):
    chrom = location.split(':')[0]
    bp_end = location.split(':')[1].split('-')[1]   
    return chrom, bp_end


def open_file_and_process(file):
    filename = get_filename(file)
    new_filename = 'locations_' + filename + '.tsv'

    with open(file) as csv_file:
        count = 0
        result_file = open(new_filename, "w")
        csv_reader = csv.DictReader(csv_file, delimiter='\t')
        fieldnames = csv_reader.fieldnames
        writer = csv.DictWriter(result_file, fieldnames=fieldnames, delimiter='\t')
        writer.writeheader()

        for row in csv_reader:
            chromosome = row[CHR_DSET].replace('23', 'X').replace('24', 'Y')
            bp = row[BP_DSET]
            rsid = row[SNP_DSET]
            # lookup missing bp locations
            if len(bp) == 0 or chromosome not in accepted_chr_values:
                # try dbSNP client:
                print('Calling dbSNP')
                dbsnpclient = dbSNPclient()
                request = dbsnpclient.bp_from_rsid(rsid)
                if request is not False:
                    row[CHR_DSET] = request[0].replace('chr','').replace('X', '23').replace('Y', '24')
                    row[BP_DSET] = request[1]
                else:
                    # otherwise try the ensembl client
                    print('calling ensembl rest api')
                    restclient = EnsemblRestClient()
                    response = restclient.resolve_location_with_rest(rsid)
                    if response is not False:
                        row[CHR_DSET], row[BP_DSET] = parse_location_data(response)
                    else:
                        row[CHR_DSET], row[BP_DSET] = 'NA', 'NA'            

            writer.writerow(row)
            count += 1
            if count % 1000 == 0:
                print(count)


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    args = argparser.parse_args()
    file = args.f

    open_file_and_process(file)

if __name__ == "__main__":
    main()
