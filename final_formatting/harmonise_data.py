import requests
import sys
import csv
import argparse
import time
import sqlite3
from pyliftover import LiftOver

sys_paths = ['sumstats/', '../sumstats/', '../../sumstats/']
sys.path.extend(sys_paths)
from utils import *


ucsc_release = {'28': 'hg10',
                '29': 'hg11',
                '30': 'hg13',
                '31': 'hg14',
                '33': 'hg15',
                '34': 'hg16',
                '35': 'hg17',
                '36': 'hg18',
                '37': 'hg19',
                '38': 'hg38'}


suffixed_release = {'28': 'NCBI28',
                    '29': 'NCBI29',
                    '30': 'NCBI30',
                    '31': 'NCBI31',
                    '33': 'NCBI33',
                    '34': 'NCBI34',
                    '35': 'NCBI35',
                    '36': 'NCBI36',
                    '37': 'GRCh37',
                    '38': 'GRCh38'}


class EnsemblRestClient(object):
    def __init__(self, server='http://rest.ensembl.org', reqs_per_sec=15):
        self.server = server
        self.reqs_per_sec = reqs_per_sec
        self.req_count = 0
        self.last_req = 0

    def perform_rest_action(self, endpoint, hdrs=None):
        hdrs = self.set_json_format(hdrs)
        self.rate_check()
        data = None

        try:
            content = requests.get(self.server + endpoint, headers=hdrs)
            if self.slow_down_if_needed(content.headers):
                content = requests.get(self.server + endpoint, headers=hdrs)
            if content:
                data = content.json()
            else:
                data = 'NA'
            self.req_count += 1

        except requests.exceptions.HTTPError as ehe:
            # check if we are being rate limited by the server
            if self.slow_down_if_needed(ehe.headers):
                self.perform_rest_action(endpoint, hdrs)
        except requests.exceptions.RequestException as e:
                print('Request failed for {0}: Status: {1}\n'.format(
                endpoint, e))

        return data

    @staticmethod
    def slow_down_if_needed(headers):
        if 'Retry-After' in headers:
            retry = headers['Retry-After']
            time.sleep(float(retry))
            return True

    @staticmethod
    def set_json_format(headers):
        if headers is None:
            headers = {}
        if 'Content-Type' not in headers:
            headers['Content-Type'] = 'application/json'
        return headers

    def rate_check(self):
        # check if we need to rate limit ourselves
        if self.req_count >= self.reqs_per_sec:
            time.sleep(1)
            delta = time.time() - self.last_req
            if delta < 1:
                time.sleep(1 - delta)
            self.last_req = time.time()
            self.req_count = 0

    def map_bp_to_build_with_rest(self, chromosome, bp, build_orig, build_mapped):
        map_build = self.perform_rest_action(
            '/map/human/{orig}/{chromosome}:{bp}:{bp}/{mapped}?'.format(
            chromosome=chromosome, bp=bp, orig=build_orig, mapped=build_mapped)
            )
        return self.retrieve_mapped_item(map_build)

    @staticmethod
    def retrieve_mapped_item(map_build):
        if "mappings" in map_build:
            mappings = map_build["mappings"]
            if len(mappings) > 0:
                if "mapped" in mappings and "start" in mappings["mapped"]:
                    return map_build["mappings"][0]["mapped"]["start"]
        return 'NA'

    def resolve_rsid(self, chromosome, bp):
        rsid_request = self.perform_rest_action(
            '/overlap/region/human/{chromosome}:{bp}:{bp}?feature=variation'.format(
            chromosome=chromosome, bp=bp)
            )
        return self.retrieve_rsid(rsid_request)

    @staticmethod
    def retrieve_rsid(rsid_request):
        if len(rsid_request) > 0 and "id" in rsid_request[0]:
            return rsid_request[0]["id"]
        return 'id:NA'

    def check_orientation_with_rest(self, rsid):
        variation_request = self.perform_rest_action(
            '/variation/human/{rsid}?'.format(
            rsid=rsid)
            )
        return self.retrieve_strand(variation_request)

    @staticmethod
    def retrieve_strand(variation_request):
        if "mappings" in variation_request:
            mappings = variation_request["mappings"]
            if len(mappings) > 0:
                if "strand" in mappings[0]:
                    return mappings[0]["strand"]
        return False


def isNumber(value):
    try:
        int(value)
        return True
    except ValueError:
        return False


# Map using pyLiftover and if that fails map using Ensembl REST API
def map_bp_to_build_via_liftover(chromosome, bp, build_map):
    if isNumber(bp):
        data = build_map.convert_coordinate('chr' + str(chromosome), int(bp))
        if len(data) > 0:
            return data[0][1]
    return None


def map_bp_to_build_via_ensembl(chromosome, bp, from_build, to_build):
    client = EnsemblRestClient()
    data = client.map_bp_to_build_with_rest(
        chromosome, bp, suffixed_release.get(from_build), suffixed_release.get(to_build)
    )
    return data


def check_if_mapping_is_needed(from_build, to_build):
    if from_build == to_build:
        return False
    return True


# sqlite database connection
def create_conn(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except NameError as e:
        print(e)
    return None


def check_orientation_with_sqlite(conn, rsid):
    cur = conn.cursor()
    cur.execute("SELECT strand FROM snp150Common WHERE name=?", (rsid,))
    data = cur.fetchone()
    if data is not None:
        return data[0]
    return False


def check_orientation(conn, rsid):
#    strand_translate = {'pos': ['+', '1'],
#                        'neg': ['-', '-1']}
    strand = check_orientation_with_sqlite(conn, rsid)
    if strand is False:
        client = EnsemblRestClient()
        strand = client.check_orientation_with_rest(rsid)
#    for name, values in strand_translate.items():
#        if strand in values:
#            strand = name
    return strand


def rev_comp_if_needed(conn, rsid, effect, other):
    neg = ['-', '-1']
    orientation = check_orientation(conn, rsid)
    #if orientation == 'neg':
    if orientation in neg:
        effect = reverse_complement(effect)
        other = reverse_complement(other)
    return effect, other


def reverse_complement(seq):
    base_complement = {'A': 'T',
                       'T': 'A',
                       'C': 'G',
                       'G': 'C'}
    rev_seq = seq[::-1].upper()
    rev_comp = []
    for base in rev_seq:
        if base in base_complement.keys():
            rev_comp.append(base_complement.get(base))
        else:
            rev_comp.append(base)
    return ''.join(rev_comp)


def open_file_and_process(file, from_build, to_build):
    filename = get_filename(file)
    new_filename = 'harmonised_' + filename + '.tsv'
    if check_if_mapping_is_needed(from_build, to_build):
        build_map = LiftOver(ucsc_release.get(from_build), ucsc_release.get(to_build))

    with open(file) as csv_file:
        result_file = open(new_filename, "w")
        csv_reader = csv.DictReader(csv_file, delimiter='\t')
        fieldnames = csv_reader.fieldnames
        writer = csv.DictWriter(result_file, fieldnames=fieldnames, delimiter='\t')

        writer.writeheader()

        database = 'snp.sqlite'
        conn = create_conn(database)

        for row in csv_reader:
            chromosome = row[CHR_DSET].replace('23', 'X').replace('24', 'Y')
            bp = row[BP_DSET]

            # do the bp location mapping if needed
            if check_if_mapping_is_needed(from_build, to_build):
                mapped_bp = map_bp_to_build_via_liftover(chromosome=chromosome, bp=bp, build_map=build_map)
                if mapped_bp is None:
                    mapped_bp = map_bp_to_build_via_ensembl(chromosome=chromosome, bp=bp, from_build=from_build, to_build=to_build)
                row[BP_DSET] = mapped_bp

            # lookup missing rsids
            bp = row[BP_DSET]
            if 'rs' not in row[SNP_DSET]:
                client = EnsemblRestClient()
                row[SNP_DSET] = client.resolve_rsid(chromosome, bp)

            print(check_orientation_with_sqlite(conn, row[SNP_DSET]))
            # delete if rsid is false
            # revcomp if needed

            #writer.writerow(row)


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    argparser.add_argument('-original', help='The build of the file', required=True)
    argparser.add_argument('-mapped', help='The build to map to', required=True)
    args = argparser.parse_args()
    file = args.f
    from_build = args.original
    to_build = args.mapped

    #open_file_and_process(file, from_build, to_build)
    database = 'snp.sqlite'
    conn = create_conn(database)
    print(rev_comp_if_needed(conn, 'rs10796459', 'ACGTN/-', 'CTAGG'))

if __name__ == "__main__":
    main()
