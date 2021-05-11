import pandas as pd
import numpy as np
import gzip
import pathlib
import argparse

# 1) read in:
#   a) get columns
#   b) get INFO and FORMAT columns
#   c) row by row iteration to assign the values to the columns
#       - convert p-values
# 2) write out the tsv

class VCF():
    def __init__(self, vcf):
        self.vcf = vcf
        self.vcf_header = ['chromosome', 'base_pair_location','variant_id','other_allele','effect_allele','QUAL','FILTER', 'INFO', 'FORMAT', 'GWAS_DATA']
        self.tsv_header = [h for h in self.vcf_header if h not in ['INFO', 'FORMAT', 'GWAS_DATA']]
        self.meta_header = []
        self.info_list = []
        self.format_list = []
        self.header_mapper = {'ES': 'effect_size',
                              'SE': 'standard_error',
                              'LP': 'neg_log10_p_value',
                              'AF': 'effect_allele_frequency',
                              'SS': 'sample_size',
                              'EZ': 'z-score',
                              'SI': 'accuracy_score',
                              'NC': 'number_of_cases',
                              'ID': 'id'
                             }

    @staticmethod
    def _append_to_list_if_starts_with(s, x, l):
        if s.startswith(x):
            l.append(s)
        return l

    def _get_INFO(self):
        for line in self.meta_header:
            self._append_to_list_if_starts_with(line, '##INFO', self.info_list)
        return self.info_list

    def _get_FORMAT(self):
        for line in self.meta_header:
            self._append_to_list_if_starts_with(line, '##FORMAT', self.format_list)
        return self.format_list

    def _set_tsv_header(self):
        self._get_meta_header()
        self._get_INFO()
        self._get_FORMAT()
        for m in self.info_list:
            self.tsv_header.append(self._meta_to_header(m))
        for m in self.format_list:
            self.tsv_header.append(self._meta_to_header(m))
        return self.tsv_header

    def _get_meta_header(self):
        gzipped = self._is_gzipped_vcf()
        if gzipped:
            with gzip.open(self.vcf, 'r') as f:
                self._set_rows_starting_with_hash(f)
        else:
            with open(self.vcf, 'r') as f:
                self._set_rows_starting_with_hash(f)
        return self.meta_header

    def _set_rows_starting_with_hash(self, f):
        still_in_meta_header = True
        while still_in_meta_header:
            line = f.readline().strip().decode('ascii')
            if str(line).startswith('#'):
                self.meta_header.append(line)
            else:
                still_in_meta_header = False

    def _get_vcf_extension(self):
        return pathlib.Path(self.vcf).suffixes

    def _is_gzipped_vcf(self):
        file_exts = self._get_vcf_extension()
        if '.gz' in file_exts:
            return True
        return False

    def _get_filename(self):
        suffix = "".join(self._get_vcf_extension())
        filename = self.vcf.replace(suffix, '.tsv.gz')
        return filename

    @staticmethod
    def _meta_to_header(meta):
        sections = meta.split(',')
        key = sections[0].split('=<ID=')[-1]
        return key

    def _write_to_tsv(self, df, header=True):
        mode = 'w' if header is True else 'a'
        tsv_out = self._get_filename()
        df.to_csv(tsv_out,
                    mode=mode,
                    header=header,
                    compression='gzip',
                    sep="\t",
                    na_rep="NA",
                    index=False)

    def _vcf_to_df_iterator(self):
        self.df_iterator = pd.read_table(self.vcf, comment='#', names=self.vcf_header, chunksize=1000000)
        return self.df_iterator

    def _parse_gwas_data(self, df):
        expanded_df = pd.concat([df.reset_index(), pd.DataFrame(
            [self._split_keys_and_values(x, y) for x, y in zip(df['FORMAT'], df['GWAS_DATA'])]
        )], axis=1, join="inner").drop(
            columns=['FORMAT', 'GWAS_DATA', 'index']
        )
        return expanded_df

    def gwas_vcf_to_gwas_tsv(self):
        self._set_tsv_header()
        self._vcf_to_df_iterator()
        header = True
        for df in self.df_iterator:
            expanded_df = self._parse_gwas_data(df)
            expanded_df.rename(columns=self.header_mapper, inplace=True)
            expanded_df['p_value'] = np.power(10, (-1 * expanded_df['neg_log10_p_value'].astype(float)))
            self._write_to_tsv(expanded_df, header)
            header = False

    @staticmethod
    def _split_keys_and_values(x, y):
        keys = x.split(':')
        values = y.split(':')
        return dict(zip(keys, values))


def convert_gwas_vcf_to_tsv(vcf):
    vcf_handler = VCF(vcf)
    vcf_handler.gwas_vcf_to_gwas_tsv()

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='Path to the file to be processed', nargs='+', required=True)
    args = argparser.parse_args()

    for vcf in args.f:
        convert_gwas_vcf_to_tsv(vcf)


if __name__ == '__main__':
    main()