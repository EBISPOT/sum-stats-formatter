import argparse
import json
import os
import pathlib
import format.peek as sspk
import format.split_column as sssp
import dask.dataframe as dd
import pandas as pd


class Table():
    def __init__(self, file, outfile_prefix, field_sep, 
            remove_starting, keep_cols, header_mapping):
        self.file = file
        self.outfile_prefix = outfile_prefix
        self.field_sep = field_sep
        self.ignore_pattern = remove_starting
        self.keep_cols = keep_cols
        self.header_mapping = header_mapping
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
        #self.header = pd.read_csv(self.file, 
        #        comment=self.ignore_pattern, 
        #        sep=self.field_sep, 
        #        nrows=0).columns.tolist()
        return self.header
        
    def dask_df(self):
        self.dd = dd.read_csv(self.file, 
                comment=self.ignore_pattern, 
                sep=self.field_sep, 
                dtype=str, 
                error_bad_lines=False, 
                warn_bad_lines=True)

    #def pandas_df(self):
    #    self.df = pd.read_csv(self.file, 
    #            comment=self.ignore_pattern, 
    #            sep=self.field_sep, 
    #            dtype=str, 
    #            index_col=False,
    #            error_bad_lines=False, 
    #            warn_bad_lines=True,
    #            chunksize=1000000)

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

    def check_f_and_r_fields(self, find_replace):
        for item in find_replace:
            if item['field'] not in self.get_header():
                print("Cannot perfom find and replace on field '{}', because field cannot be found".format(item['field']))
                return False
        return True
        
    def perform_find_replacements(self, find_replace):
        for item in find_replace:
            self.find_and_replace(item['field'], item['find'], item['replace'])

        




def parse_config(json_config):
    try:
        with open(json_config, 'r') as f:
            config = json.load(f)
            config["outFilePrefix"] = set_var_from_dict(config, "outFilePrefix", "formatted_") 
            config["separator"] = set_var_from_dict(config, "separator", "\s+") 
            config["removeLinesStarting"] = set_var_from_dict(config, "removeLinesStarting", "#") 
            config["keepColumns"] = set_var_from_dict(config, "keepColumns", None) 
            config["headerRename"] = set_var_from_dict(config, "headerRename", None) 
            return config
    except FileNotFoundError:
        print("JSON config: {} was not found".format(json_config))
    except json.decoder.JSONDecodeError:
        print("JSON config: {} could not be understood".format(json_config))


def set_var_from_dict(dictionary, var_name, default):
    return dictionary[var_name] if var_name in dictionary else default


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The name of the file to be processed', required=True)
    #argparser.add_argument('-dir', help='The name of the directory containing the files that need to processed')
    #argparser.add_argument('-concat', help='Concatenate the files first (if possible)', action='store_true', default='store_false')
    argparser.add_argument('-config', help='The name of the configuration file')
    args = argparser.parse_args()
    config = {}
    print(sspk.peek(args.f))
    if not args.config:
        print("no configuration provided")
    else:
        config = parse_config(args.config)
        table = Table(args.f, config["outFilePrefix"], 
                config["separator"], config["removeLinesStarting"], 
                config["keepColumns"], config["headerRename"])
        table.dask_df()
        table.field_names.extend(table.get_header())
        # check for splits request
        splits = config['splitColumns'] if config['splitColumns'] else None
        if splits:
            if table.check_split_name_clashes(splits):
                table.perform_splits(splits)

        #find and replace
        find_replace = config['findAndReplaceValue'] if config['findAndReplaceValue'] else None
        if find_replace:
            if table.check_f_and_r_fields(find_replace):
                table.perform_find_replacements(find_replace)
        #rename columns
        #keep cols
        table.to_csv()
        #md5
        print(sspk.peek(table.outfile_name))


    #if config["splitColumns"]:
    #    for col in config["splitColumns"]:
    #        spl.split(file=args.f, 
    #                old_header=col["field"], 
    #                left_header=col["leftName"], 
    #                right_header=col["rightName"], 
    #                delimiter=col["separator"])










if __name__ == "__main__":
    main()
