import argparse
import json
import os
import hashlib
import pathlib
from tabulate import tabulate
import format.peek as sspk
import format.split_column as sssp
import format.tab_man_gui as tmg
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import pandas as pd
from format.utils import header_mapper

pbar = ProgressBar()
pbar.register()

class Table():
    def __init__(self, file, outfile_prefix, field_sep, remove_starting):
        self.file = file
        self.outfile_prefix = outfile_prefix
        self.field_sep = field_sep
        self.ignore_pattern = remove_starting
        self.field_names = []

    def get_extension(self):
        self.file_extension = "".join(pathlib.Path(self.file).suffixes)

    def get_filename(self):
        self.get_extension()
        self.filename = pathlib.Path(self.file).name.replace(self.file_extension, '')
        
    def get_parent_dir(self):
        self.parent_dir = pathlib.Path(self.file).parent

    def get_header(self):
        self.header = self.dd.columns.tolist()
        return self.header
        
    def dask_df(self):
        if self.ignore_pattern:
            self.dd = dd.read_csv(self.file, 
                comment=self.ignore_pattern, 
                sep=self.field_sep, 
                dtype=str, 
                error_bad_lines=False, 
                warn_bad_lines=True)
        else:
            self.dd = dd.read_csv(self.file, 
                sep=self.field_sep, 
                dtype=str, 
                error_bad_lines=False, 
                warn_bad_lines=True)


    def partial_df(self, nrows=10):
        if self.ignore_pattern:
            self.dd = pd.read_csv(self.file, 
                comment=self.ignore_pattern, 
                sep=self.field_sep, 
                dtype=str, 
                error_bad_lines=False, 
                warn_bad_lines=True, nrows=nrows)
        else:
            self.dd = pd.read_csv(self.file, 
                sep=self.field_sep, 
                dtype=str, 
                error_bad_lines=False, 
                warn_bad_lines=True, nrows=nrows)


    def fill_blanks_with_na(self):
        self.dd = self.dd.replace('', 'NA')

    def set_outfile_name(self):
        self.get_filename()
        self.get_parent_dir()
        self.outfile_name = os.path.join(self.parent_dir, self.outfile_prefix + self.filename + '.tsv')

    def to_csv(self):
        self.set_outfile_name()
        self.fill_blanks_with_na()
        self.dd.to_csv(self.outfile_name, 
                mode='w', 
                header=True, 
                sep="\t", 
                na_rep="NA", 
                index=False, 
                single_file=True)

    def split_column(self, field, delimiter, left_name, right_name):
        self.dd = sssp.split_field(df=self.dd, 
                field=field, 
                delimiter=delimiter, 
                left_name=left_name, 
                right_name=right_name)

    def find_and_replace(self, field, find, replace):
        self.dd[field] = self.dd[field].str.replace(r'{}'.format(find), replace)

    def check_split_name_clashes(self, splits):
        for split in splits:
            if split['field'] not in self.field_names:
                print("The specified field '{}' for splitting on was not found".format(split['field']))
                return False
            if split['leftName'] in self.field_names:
                print("The split on field '{}' cannot be done because the left header, '{}', clashes with an existing header".format(split['field'], split['leftName']))
                return False
            if split['rightName'] in self.field_names:
                print("The split on field '{}' cannot done because the right header, '{}', clashes with an existing header".format(split['field'], split['rightName']))
                return False
            if split['leftName'] == split['rightName']:
                print("The split on field '{}' cannot done because the right and left headers, '{}', clash with each other".format(split['field'], split['rightName']))
                return False
            else:
                self.field_names.extend([split['leftName'], split['rightName']])
        return True

    def perform_splits(self, splits):         
        for split in splits:
            self.split_column(split['field'], split['delimiter'], split['leftName'], split['rightName'])

    def perform_find_replacements(self, find_replace):
        for item in find_replace:
            if item['field'] in self.get_header():
                self.find_and_replace(item['field'], item['find'], item['replace'])

    def perform_header_rename(self, header_rename):
        try:
            self.dd = self.dd.rename(columns=header_rename)
            return True
        except KeyError as e:
            print(e)
            return False

    def perform_keep_cols(self, keep_cols):
        keep_cols = [c for c in keep_cols if c in self.get_header()]
        self.dd = self.dd[keep_cols]

    def peek(self, keep_cols=False):
        if keep_cols:
            keep_cols = [c for c in keep_cols if c in self.get_header()]
            return tabulate(self.dd.head(10)[keep_cols], headers='keys', tablefmt="fancy_grid", disable_numparse=True, showindex=False)
        else:
            return tabulate(self.dd.head(10), headers='keys', tablefmt="fancy_grid", disable_numparse=True, showindex=False)

    


def parse_config(json_config):
    try:
        with open(json_config, 'r') as f:
            config = json.load(f)
            config["outFilePrefix"] = set_var_from_dict(config, "outFilePrefix", "formatted_") 
            config["fieldSeparator"] = set_var_from_dict(config, "fieldSeparator", "\s+") 
            config["removeComments"] = set_var_from_dict(config, "removeComments", "#") 
            return config
    except FileNotFoundError:
        print("JSON config: {} was not found".format(json_config))
    except json.decoder.JSONDecodeError:
        print("JSON config: {} could not be understood".format(json_config))


def set_var_from_dict(dictionary, var_name, default):
    return dictionary[var_name] if var_name in dictionary else default


def md5sum(file):
    hash_md5 = hashlib.md5()
    with open(file, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def apply_config_to_file(file, config):
    table = Table(file, config["outFilePrefix"], config["fieldSeparator"], config["removeComments"])
    table.dask_df()
    table.field_names.extend(table.get_header())

    # check for splits request
    splits = set_var_from_dict(config, 'splitColumns', None)
    if splits:
        if table.check_split_name_clashes(splits):
            table.perform_splits(splits)

    #find and replace
    find_replace = []
    column_config = set_var_from_dict(config, 'columnConfig', None)
    for field in column_config:
        if "find" and "replace" in field:
            find_replace.append(field)
    if find_replace:
        table.perform_find_replacements(find_replace)

    #rename columns
    header_rename = {}
    for field in column_config:
        if "rename" in field:
            if field["rename"]:
                header_rename[field["field"]] = field["rename"]
    if header_rename:
        table.perform_header_rename(header_rename)

    #keep cols
    keep_cols = []
    for field in column_config:
        if "keep" in field:
            if field["keep"]:
                field_name = field["rename"] if "rename" in field and len(field["rename"]) > 0 else field["field"]
                keep_cols.append(field_name)
    if keep_cols:
        table.perform_keep_cols(keep_cols)
    table.to_csv()

    #md5
    if config['md5']:
        md5 = md5sum(table.outfile_name)
        md5_outfile = table.outfile_name + '.md5'
        with open(md5_outfile, 'w') as f:
            f.write(md5)

    print("-------------- File out --------------")
    print(sspk.peek(table.outfile_name))



def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=False)
    argparser.add_argument('-gui', help='If you want the GUI', action='store_true', default='store_false', required=False)
    #argparser.add_argument('-dir', help='The name of the directory containing the files that need to processed')
    #argparser.add_argument('-concat', help='Concatenate the files first (if possible)', 
    #                        action='store_true', default='store_false')
    argparser.add_argument('-config', help='The name of the configuration file')
    args = argparser.parse_args()

    config = {}
    if args.f:
        print("-------------- File in --------------")
        print(sspk.peek(args.f))
        if not args.config:
            print("no configuration provided")
        else:
            config = parse_config(args.config)
            apply_config_to_file(args.f, config)
    if args.gui is True:
        tmg.main()
    else:
        print("Please provide some argunents")
        argparser.print_help()


if __name__ == "__main__":
    main()
