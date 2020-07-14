import argparse
import json
import os
import pandas as pd
import numpy as np
import pandas.io.formats.excel
from bsub import bsub
import sys
import pathlib
import hashlib
import format.peek as sspk
import format.split_column as sssp
from format.utils import header_mapper
from format.common_constants import *


SEP_MAP = {
            'space': '\\s+', 
            'tab': '\\t',
            'comma': ',',
            'pipe': '|'
           }

MAND_COLS = (PVAL_DSET, CHR_DSET, BP_DSET, OR_DSET, RANGE_L_DSET, RANGE_U_DSET, 
            BETA_DSET, SE_DSET, FREQ_DSET , EFFECT_DSET, OTHER_DSET, SNP_DSET)

def generate_config_template(table, config_name, config_type):
    table.partial_df() #initialise heead of table
    columns_in = table.get_fields()
    sep = table.get_sep()
    config = Config(columns_in=columns_in, config_file=config_name, config_type=config_type, field_sep=sep)
    config.generate_config_template()
        
def parse_config(config_name, config_type):
    config = Config(config_file=config_name, config_type=config_type)
    config.parse_config()
    return config.config


class Table():
    def __init__(self, file, field_sep='\s+', outfile_prefix='outfile_', remove_starting=None):
        self.file = file
        self.outfile_prefix = outfile_prefix
        self.field_sep = field_sep
        self.ignore_pattern = remove_starting
        self.field_names = []
        print("field sep: {}".format(self.field_sep))

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
                warn_bad_lines=True)
        else:
            self.pd = pd.read_csv(self.file,
                sep=self.field_sep,
                dtype=str,
                error_bad_lines=False,
                warn_bad_lines=True)


    def partial_df(self, nrows=10):
        if self.ignore_pattern:
            self.pd = pd.read_csv(self.file,
                comment=self.ignore_pattern,
                sep=self.field_sep,
                dtype=str,
                error_bad_lines=False,
                warn_bad_lines=True, nrows=nrows)
        else:
            self.pd = pd.read_csv(self.file,
                sep=self.field_sep,
                dtype=str,
                error_bad_lines=False,
                warn_bad_lines=True, nrows=nrows)


    def set_outfile_name(self, preview=False):
        self.get_filename()
        self.get_parent_dir()
        if preview is False:
            self.outfile_name = os.path.join(self.parent_dir, self.outfile_prefix + self.filename + '.tsv')
        else:
            self.outfile_name = os.path.join(self.parent_dir, self.outfile_prefix + self.filename + '.PREVIEW.tsv')


    def to_csv(self, preview=False):
        self.set_outfile_name(preview)
        self.pd.to_csv(self.outfile_name,
                mode='w',
                header=True,
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

    def peek(self, keep_cols=False):
        if keep_cols:
            keep_cols = [c for c in keep_cols if c in self.get_header()]
            return tabulate(self.pd.head(10)[keep_cols], headers='keys', tablefmt="fancy_grid", disable_numparse=True, showindex=False)
        else:
            return tabulate(self.pd.head(10), headers='keys', tablefmt="fancy_grid", disable_numparse=True, showindex=False)


class Config():
    def __init__(self, columns_in=None, config_file='tabman_config.json', config_type='xlsx', field_sep='\s+'):
        self.columns_in = columns_in
        self.config_file = config_file
        self.config_type = config_type
        self.field_sep = field_sep
        self.config = { 
                        "outFilePrefix":"formatted_",
                        "md5":"True",
                        "fieldSeparator":self.field_sep,
                        "removeLinesStarting":"",
                        "splitColumns":[],
                        "columnConfig":[]
                     }
       

    def generate_config_template(self):
        script_dir = os.path.dirname(os.path.realpath(__file__))
        self.file_config = pd.read_excel(os.path.join(script_dir,"tab_man_template.xlsx"), sheet_name="file")
        self.splits_config = pd.read_excel(os.path.join(script_dir,"tab_man_template.xlsx"), sheet_name="splits")
        self.find_replace_config = pd.read_excel(os.path.join(script_dir,"tab_man_template.xlsx"), sheet_name="find_and_replace")
        self.columns_in_df = pd.DataFrame(self.columns_in, columns=['IN']).rename(columns={"IN":"FIELD"})
        self.columns_out_config = self.suggest_header_mapping()
        if self.config_type == 'xlsx':
            self.generate_xlsx_config()
        elif self.config_type == 'json':
            self.generate_json_config()


    def parse_config(self):
        if self.config_type == 'xlsx':
            self.parse_xlsx_config()
        elif self.config_type == 'json':
            self.parse_json_config()


    def generate_xlsx_config(self):
        xlsx_out = self.config_file
        writer = pd.ExcelWriter(xlsx_out, engine ='xlsxwriter')
        self.file_config.to_excel(writer, index=False, sheet_name="file")

        workbook  = writer.book
        file_sheet = writer.sheets['file']

        # define cell formats
        text_format = workbook.add_format()
        text_format.set_num_format('@')       

        file_sheet.set_column('A:C', 18, text_format)
        file_sheet.data_validation('B2', {'validate': 'list',
                                  'source': ['space', 'tab', 'comma', 'pipe']})
        file_sheet.data_validation('B6', {'validate': 'list',
                                  'source': ['True', 'False']})
        self.splits_config = self.splits_config.append(self.columns_in_df, ignore_index=True, sort=True)
        self.splits_config.to_excel(writer, index=False, sheet_name="splits")
        self.find_replace_config = self.find_replace_config.append(self.columns_in_df, ignore_index=True, sort=True)
        self.find_replace_config.to_excel(writer, index=False, sheet_name="find_and_replace", columns=['FIELD','FIND','REPLACE','EXTRACT'])
        self.columns_out_config.to_excel(writer, index=False, sheet_name="columns_out")
        
        splits_sheet = writer.sheets['splits']
        find_and_replace_sheet = writer.sheets['find_and_replace']
        columns_out_sheet = writer.sheets['columns_out']

        splits_sheet.set_column('A:D', 18, text_format)
        find_and_replace_sheet.set_column('A:D', 18, text_format)
        columns_out_sheet.set_column('A:B', 18, text_format)

        writer.save()

    def suggest_header_mapping(self):
        columns_out = {}
        for field in self.columns_in:
            if field in header_mapper.keys():
                columns_out[field] = field
            else:
                for key, value_list in header_mapper.items():
                    if field in value_list and key not in columns_out.values():
                        columns_out[field] = key
                        break
                    else:
                        columns_out[field] = None
        mandatory_fields_not_in_columns = []
        for mandatory_field in MAND_COLS:
            if mandatory_field not in columns_out and mandatory_field not in columns_out.values():
                mandatory_fields_not_in_columns.append(mandatory_field)
        cols_df = pd.DataFrame(columns_out.items(), columns=['IN', 'OUT'])
        cols_df = cols_df.append(pd.DataFrame(mandatory_fields_not_in_columns, columns=['OUT']), ignore_index=True, sort=True)
        return cols_df

    def generate_json_config(self):
        pass

    def parse_xlsx_config(self):
        try:
            self.file_config = pd.read_excel(self.config_file, dtype=str, sheet_name="file", usecols=['ATTRIBUTE', 'VALUE']).dropna().set_index('ATTRIBUTE')['VALUE'].to_dict()
            self.splits_config = pd.read_excel(self.config_file, dtype=str, sheet_name="splits").dropna().rename(columns={"FIELD": "field", "SEPARATOR": "separator", "LEFT_NAME":"leftName", "RIGHT_NAME": "rightName"}).to_dict('records')
            self.find_replace_config = pd.read_excel(self.config_file, dtype=str, sheet_name="find_and_replace")
            self.columns_out_config = pd.read_excel(self.config_file, dtype=str, sheet_name="columns_out").rename(columns={"IN":"FIELD", "OUT":"OUT_NAME"}).dropna(subset=['OUT_NAME'])
            self.column_config = pd.merge(self.find_replace_config, self.columns_out_config, how='right', on='FIELD').replace({np.nan: ""}).rename(columns={"FIELD": "field", "FIND": "find", "REPLACE": "replace", "EXTRACT": "extract", "OUT_NAME": "rename"}).to_dict('records')
            print(self.column_config)
            self.config["outFilePrefix"] = set_var_from_dict(self.file_config, "outFilePrefix", "formatted_") 
            self.config["fieldSeparator"] = set_var_from_dict(self.file_config, "fieldSeparator", self.field_sep) 
            self.config["fieldSeparator"] = SEP_MAP[self.config["fieldSeparator"]] if self.config["fieldSeparator"] in SEP_MAP else self.config["fieldSeparator"]
            self.config["removeComments"] = set_var_from_dict(self.file_config, "removeComments", "") 
            self.config["splitColumns"].extend(self.splits_config)
            self.config["columnConfig"].extend(self.column_config)
            print(self.config)
            
        except FileNotFoundError:
            print("XLSX config: {} was not found".format(self.config_file))

    def parse_json_config(self):
        try:
            with open(self.config_file, 'r') as f:
                config = json.load(f)
                config["outFilePrefix"] = set_var_from_dict(config, "outFilePrefix", "formatted_") 
                config["fieldSeparator"] = set_var_from_dict(config, "fieldSeparator", self.field_sep) 
                config["removeComments"] = set_var_from_dict(config, "removeComments", "") 
                return config
        except FileNotFoundError:
            print("JSON config: {} was not found".format(self.config_file))
        except json.decoder.JSONDecodeError:
            print("JSON config: {} could not be understood".format(self.config_file))
    

def set_var_from_dict(dictionary, var_name, default):
    return dictionary[var_name] if var_name in dictionary else default


def md5sum(file):
    hash_md5 = hashlib.md5()
    with open(file, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def apply_config_to_file(file, config, preview=False):
    table = Table(file, outfile_prefix=config["outFilePrefix"], field_sep=config["fieldSeparator"], remove_starting=config["removeComments"])
    table.partial_df() if preview is True else table.pandas_df() 
    table.field_names.extend(table.get_header())

    print(preview)
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

    #extract
    extract = []
    column_config = set_var_from_dict(config, 'columnConfig', None)
    for field in column_config:
        if "extract" in field:
            extract.append(field)
    if extract:
        table.perform_extract(extract)

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
        field_name = field["rename"] if "rename" in field and len(field["rename"]) > 0 else field["field"]
        keep_cols.append(field_name)
    if keep_cols:
        table.perform_keep_cols(keep_cols)
    table.to_csv(preview)

    #md5
    if config['md5'] is True:
        md5 = md5sum(table.outfile_name)
        md5_outfile = table.outfile_name + '.md5'
        with open(md5_outfile, 'w') as f:
            f.write(md5)

    print("-------------- File out --------------")
    print(sspk.peek(table.outfile_name))


def apply_config_to_file_use_cluster(file, config):
    sub = bsub("gwas_ss_format", M="36000", R="rusage[mem=36000]", N="")
    command = "tabman -f {} -config {} -mode apply".format(file, config)
    print(">>>> Submitting job to cluster, job id below")
    print(sub(command).job_id)
    print("You will receive an email when the job is finished. Formatted files, md5sums and configs will appear in the same directory as the input file.")


def env_variable_else(env_var_name, default):
        return os.environ.get(env_var_name) if os.environ.get(env_var_name) else default

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='Path to the file to be processed', required=True)
    argparser.add_argument('-sep', help='The seperator/delimiter of the input file used to seperate the fields', default='space', choices=['space', 'tab', 'comma', 'pipe'], required=False)
    argparser.add_argument('-preview', help='Show a preview (top 10 lines) of the input/output file(s)', action='store_true', required=False)
    argparser.add_argument('-config', help='Name configuration file. You can set a path to the configuration file directory in the environment variable SS_FORMAT_CONFIG_DIR', required=False)
    argparser.add_argument('-config_type', help='Type of configuration file', default='xlsx', choices=['xlsx', 'json'], required=False)
    argparser.add_argument('-mode', help='"gen" to generate the configuration file, "apply" to apply the configuration file', choices=['gen', 'apply', 'apply-cluster'], required=False)
    args = argparser.parse_args()

    config = {}    
    
    sep = SEP_MAP[args.sep]

    if not args.mode:
        print("Using field separator: {}".format(sep))
        print("-------------- File preview--------------")
        print(sspk.peek(args.f, sep=sep))
        print("Please provide some other argunents if you want to format a file")
    elif args.mode:
        if not args.config:
            print("Please provide a config file with '-config'")
            argparser.print_help()
            sys.exit()
        else:
            config_path = os.path.join(env_variable_else('SS_FORMAT_CONFIG_DIR', './'), args.config)
            if args.mode == 'gen':
                table = Table(args.f, field_sep=sep)
                print("Generating configuration template...")
                generate_config_template(table, config_path, args.config_type)
            elif args.mode == 'apply':
                print("Parsing config...")
                config_dict = parse_config(config_path, args.config_type)
                print("Applying configuration...")
                apply_config_to_file(args.f, config_dict, args.preview)
            elif args.mode == 'apply-cluster':
                print("Applying configuration using cluster job...")                
                apply_config_to_file_use_cluster(args.f, config_path)
    else:
        print("Please provide some argunents")
        argparser.print_help()


if __name__ == '__main__':
    main()
