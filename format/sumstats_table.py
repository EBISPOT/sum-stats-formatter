import os
import pathlib
import pandas as pd
import numpy as np
import format.helpers.split_column as sssp
from tabulate import tabulate


class Table:
    def __init__(self, file=None, field_sep='\s+', outfile_prefix='outfile_', remove_starting=None, dataframe=None):
        self.file = file
        self.pd = dataframe
        self.outfile_prefix = outfile_prefix
        self.field_sep = field_sep
        self.ignore_pattern = remove_starting
        self.field_names = []
        self.file_extension = ""
        self.filename = None
        self.parent_dir = None
        self.outfile_name = None

    def get_fields(self):
        self.header = self.pd.columns.tolist()
        return self.header

    def get_sep(self):
        return self.field_sep

    def get_extension(self):
        extensions = pathlib.Path(self.file).suffixes
        common_exts = [".tsv", ".csv", ".psv", ".txt"]
        if len(extensions) == 1:
            self.file_extension = extensions[0]
        elif len(extensions) > 1:
            ext = [i for i in common_exts if i in extensions]
            if len(ext) == 1:
                i = extensions.index(ext[0])
                self.file_extension = "".join(extensions[i:])
            else:
                self.file_extension = extensions[-1]
        else:
            self.file_extension = ""

    def get_filename(self):
        self.get_extension()
        self.filename = pathlib.Path(self.file).name.replace(self.file_extension, '')

    def get_parent_dir(self):
        self.parent_dir = pathlib.Path(self.file).parent

    def get_header(self):
        self.header = self.pd.columns.tolist()
        return self.header

    def pandas_df(self):
        if self.ignore_pattern:
            self.pd = pd.read_csv(self.file,
                                  comment=self.ignore_pattern,
                                  sep=self.field_sep,
                                  dtype=str,
                                  error_bad_lines=False,
                                  warn_bad_lines=True,
                                  engine='python')
        else:
            self.pd = pd.read_csv(self.file,
                                  sep=self.field_sep,
                                  dtype=str,
                                  error_bad_lines=False,
                                  warn_bad_lines=True,
                                  engine='python')

    def partial_df(self, nrows=10):
        if self.ignore_pattern:
            self.pd = pd.read_csv(self.file,
                                  comment=self.ignore_pattern,
                                  sep=self.field_sep,
                                  dtype=str,
                                  error_bad_lines=False,
                                  warn_bad_lines=True,
                                  nrows=nrows,
                                  engine='python')
        else:
            self.pd = pd.read_csv(self.file,
                                  sep=self.field_sep,
                                  dtype=str,
                                  error_bad_lines=False,
                                  warn_bad_lines=True,
                                  nrows=nrows,
                                  engine='python')

    def drop_cols(self, cols):
        print('-------')
        print(self.field_sep)
        print(self.pd.columns)
        print('-------')
        print(cols)
        self.pd.drop(columns=cols, inplace=True)

    def set_outfile_name(self, preview=False):
        self.get_filename()
        self.get_parent_dir()
        if preview is False:
            self.outfile_name = os.path.join(self.parent_dir, self.outfile_prefix + self.filename + '.tsv')
        else:
            self.outfile_name = os.path.join(self.parent_dir, self.outfile_prefix + self.filename + '.PREVIEW.tsv')

    def to_csv(self, preview=False, header=True):
        self.set_outfile_name(preview)
        if header is True:
            self.pd.to_csv(self.outfile_name,
                           mode='w',
                           header=header,
                           sep="\t",
                           na_rep="NA",
                           index=False)
        else:
            self.pd.to_csv(self.outfile_name,
                           mode='a',
                           header=header,
                           sep="\t",
                           na_rep="NA",
                           index=False)

    def split_column(self, field, delimiter, left_name, right_name):
        self.pd = sssp.split_field(df=self.pd,
                                   field=field,
                                   delimiter=delimiter,
                                   left_name=left_name,
                                   right_name=right_name)

    def find_and_replace(self, field, find, replace):
        self.pd[field] = self.pd[field].str.replace(r'{}'.format(find), r'{}'.format(replace))

    def extract(self, field, extract):
        self.pd[field] = self.pd[field].str.extract(r'({})'.format(extract))

    def check_split_name_clashes(self, splits):
        for split in splits:
            if split['field'] not in self.field_names:
                print("The specified field '{}' for splitting on was not found".format(split['field']))
                return False
            if split['leftName'] in self.field_names:
                print("The split on field '{}' cannot be done because the left header, '{}', clashes with an existing "
                      "header".format(split['field'], split['leftName']))
                return False
            if split['rightName'] in self.field_names:
                print("The split on field '{}' cannot done because the right header, '{}', clashes with an existing "
                      "header".format(split['field'], split['rightName']))
                return False
            if split['leftName'] == split['rightName']:
                print("The split on field '{}' cannot done because the right and left headers, '{}', clash with each "
                      "other".format(split['field'], split['rightName']))
                return False
            else:
                self.field_names.extend([split['leftName'], split['rightName']])
        return True

    def perform_splits(self, splits):
        for split in splits:
            self.split_column(split['field'], split['separator'], split['leftName'], split['rightName'])

    def perform_find_replacements(self, find_replace):
        for item in find_replace:
            if item['field'] in self.get_header() and item['find']:
                self.find_and_replace(item['field'], item['find'], item['replace'])

    def perform_extract(self, extract):
        for item in extract:
            if item['field'] in self.get_header() and item['extract']:
                self.extract(item['field'], item['extract'])

    def perform_header_rename(self, header_rename):
        try:
            self.pd = self.pd.rename(columns=header_rename)
            return True
        except KeyError as e:
            print(e)
            return False

    def perform_keep_cols(self, keep_cols):
        add_cols = [c for c in keep_cols if c not in self.get_header()]
        keep_cols = [c for c in keep_cols if c in self.get_header()]
        self.pd = self.pd[keep_cols]
        for c in add_cols:
            self.pd[c] = "NA"

    def peek(self, keep_cols=None):
        if keep_cols:
            keep_cols = [c for c in keep_cols if c in self.get_header()]
            return tabulate(self.pd.head(10)[keep_cols],
                            headers='keys',
                            tablefmt="fancy_grid",
                            disable_numparse=True,
                            showindex=False)
        else:
            return tabulate(self.pd.head(10),
                            headers='keys',
                            tablefmt="fancy_grid",
                            disable_numparse=True,
                            showindex=False)

    def convert_neg_log10_pvalues(self):
        self.pd.rename(columns={'p_value': 'neg_log10_p_value'}, inplace=True)
        self.pd['p_value'] = np.power(10, (-1 * self.pd['neg_log10_p_value'].astype(float)))
