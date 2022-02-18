import os
import sys
import json
import pandas as pd
import numpy as np
import jsonschema
from format.utils import SEP_MAP, MAND_COLS, set_var_from_dict, CONFIG_SCHEMA
from format.helpers.utils import header_mapper


class Config:
    def __init__(self, columns_in=None, config_file='tabman_config.json', config_type='xlsx', field_sep='\s+'):
        self.columns_in = columns_in
        self.config_file = config_file
        self.config_type = config_type
        self.field_sep = field_sep
        self.config = {
            "outFilePrefix": "formatted_",
            "md5": False,
            "convertNegLog10Pvalue": False,
            "fieldSeparator": self.field_sep,
            "removeComments": "",
            "splitColumns": [],
            "columnConfig": []
        }
        self.file_config = None
        self.splits_config = None
        self.find_replace_config = None
        self.columns_in_df = None
        self.columns_out_config = None
        self.cols_to_drop = None
        self.column_config = None

    def generate_config_template(self):
        self.file_config = pd.read_excel(os.path.join(sys.prefix, "data_files", "formatter_config_template.xlsx"),
                                         sheet_name="file")
        self.splits_config = pd.read_excel(os.path.join(sys.prefix, "data_files", "formatter_config_template.xlsx"),
                                           sheet_name="splits")
        self.find_replace_config = pd.read_excel(
            os.path.join(sys.prefix, "data_files", "formatter_config_template.xlsx"), sheet_name="find_and_replace")
        self.columns_in_df = pd.DataFrame(self.columns_in, columns=['IN']).rename(columns={"IN": "FIELD"})
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
        writer = pd.ExcelWriter(xlsx_out, engine='xlsxwriter')
        self.file_config.to_excel(writer, index=False, sheet_name="file")

        workbook = writer.book
        file_sheet = writer.sheets['file']

        # define cell formats
        text_format = workbook.add_format()
        text_format.set_num_format('@')

        file_sheet.set_column('A:C', 18, text_format)
        file_sheet.data_validation('B2', {'validate': 'list',
                                          'source': ['space', 'tab', 'comma', 'pipe']})
        file_sheet.data_validation('B5', {'validate': 'list',
                                          'source': ['True', 'False']})
        file_sheet.data_validation('B6', {'validate': 'list',
                                          'source': ['True', 'False']})

        sep_label = [k for k, v in SEP_MAP.items() if v == self.field_sep][0]
        file_sheet.write('B2', sep_label)
        self.splits_config = self.splits_config.append(self.columns_in_df, ignore_index=True, sort=True)
        self.splits_config.to_excel(writer, index=False, sheet_name="splits")
        self.find_replace_config = self.find_replace_config.append(self.columns_in_df, ignore_index=True, sort=True)
        self.find_replace_config.to_excel(writer, index=False, sheet_name="find_and_replace",
                                          columns=['FIELD', 'FIND', 'REPLACE', 'EXTRACT'])
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
        cols_df = cols_df.append(pd.DataFrame(mandatory_fields_not_in_columns, columns=['OUT']), ignore_index=True,
                                 sort=True)
        return cols_df

    def generate_json_config(self):
        json_out = self.config_file
        self.config["fieldSeparator"] = [k for k, v in SEP_MAP.items() if v == self.field_sep][0]
        self.config["splitColumns"] = self.splits_config_template()
        self.config["columnConfig"] = self.column_config_template()
        with open(json_out, 'w', encoding='utf-8') as f:
            json.dump(self.config, f, ensure_ascii=False, indent=4)

    def splits_config_template(self):
        template = []
        for col in self.columns_in_df['FIELD'].to_list():
            template.append({"field": col, "leftName": "", "rightName": "", "separator": ""})
        return template

    def column_config_template(self):
        template = []
        self.columns_out_config.rename(columns={'IN': 'field', 'OUT': 'rename'}, inplace=True)
        for d in self.columns_out_config.to_dict('index').values():
            d["find"] = ""
            d["replace"] = ""
            d["extract"] = ""
            template.append(d)
        return template

    def parse_xlsx_config(self):
        try:
            # read file config
            self.file_config = pd.read_excel(
                self.config_file,
                dtype=str,
                sheet_name="file",
                usecols=['ATTRIBUTE', 'VALUE']
            ).dropna().set_index('ATTRIBUTE')['VALUE'].to_dict()
            # read splits config
            self.splits_config = pd.read_excel(
                self.config_file,
                dtype=str,
                sheet_name="splits"
            ).dropna().rename(
                columns={"FIELD": "field",
                         "SEPARATOR": "separator",
                         "LEFT_NAME": "leftName",
                         "RIGHT_NAME": "rightName"}
            ).to_dict('records')
            # read find and replace config
            self.find_replace_config = pd.read_excel(
                self.config_file,
                dtype=str,
                sheet_name="find_and_replace"
            )
            # read column config
            col_config_df = pd.read_excel(
                self.config_file,
                dtype=str,
                sheet_name="columns_out"
            ).rename(
                columns={"IN": "FIELD",
                         "OUT": "OUT_NAME"}
            )

            self.columns_out_config = col_config_df.dropna(subset=['OUT_NAME'])
            self.cols_to_drop = (
                col_config_df.loc[~col_config_df.index.isin(
                    self.columns_out_config.index
                )].dropna(
                    subset=['FIELD']
                )['FIELD'].to_list()
            )
            self.column_config = pd.merge(
                self.find_replace_config,
                self.columns_out_config,
                how='right',
                on='FIELD'
            ).replace({np.nan: ""}).rename(
                columns={"FIELD": "field",
                         "FIND": "find",
                         "REPLACE": "replace",
                         "EXTRACT": "extract",
                         "OUT_NAME": "rename"}
            ).to_dict('records')

            self.config["outFilePrefix"] = set_var_from_dict(self.file_config, "outFilePrefix", "formatted_")
            self.config["fieldSeparator"] = set_var_from_dict(self.file_config, "fieldSeparator", self.field_sep)
            if self.config["fieldSeparator"] in SEP_MAP:
                self.config["fieldSeparator"] = SEP_MAP[self.config["fieldSeparator"]]
            self.config["removeComments"] = set_var_from_dict(self.file_config, "removeComments", "")
            self.config["md5"] = set_var_from_dict(self.file_config, "md5", False)
            self.config["convertNegLog10Pvalue"] = set_var_from_dict(self.file_config, "convertNegLog10Pvalue", False)
            self.config["splitColumns"].extend(self.splits_config)
            self.config["columnConfig"].extend(self.column_config)
            self.config["dropCols"] = self.cols_to_drop
            self.add_split_cols_to_out_cols(self.config["columnConfig"])
        except FileNotFoundError:
            print("XLSX config: {} was not found".format(self.config_file))

    @staticmethod
    def validate_json_config(json_config):
        try:
            jsonschema.validate(instance=json_config, schema=CONFIG_SCHEMA)
        except jsonschema.exceptions.ValidationError as e:
            sys.exit(e)
        return True

    def clean_json_config(self):
        # remove empty strings where necessary
        self.config["splitColumns"] = [c for c in self.config["splitColumns"] if len(c['separator']) > 0]
        self.cols_to_drop = [c["field"] for c in self.config["columnConfig"] if c["rename"] is None]
        clean_column_config = []
        for c in self.config["columnConfig"]:
            if c["rename"] is not None:
                clean_column_config.append(c)
        # option to call add_split_cols_to_out_cols(clean_column_config) here if required
        self.config["columnConfig"] = clean_column_config
        self.add_split_cols_to_out_cols(self.config["columnConfig"])

    def add_split_cols_to_out_cols(self, column_config):
        for c in self.config["splitColumns"]:
            if len(c["leftName"]) > 0:
                left_field = {"field": c["leftName"], "rename": ""}
                column_config.append(left_field)
            if len(c["rightName"]) > 0:
                right_field = {"field": c["rightName"], "rename": ""}
                column_config.append(right_field)

    def parse_json_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
                if self.validate_json_config(self.config):
                    self.clean_json_config()
                    self.config["outFilePrefix"] = set_var_from_dict(self.config, "outFilePrefix", "formatted_")
                    self.config["fieldSeparator"] = set_var_from_dict(self.config, "fieldSeparator", self.field_sep)
                    if self.config["fieldSeparator"] in SEP_MAP:
                        self.config["fieldSeparator"] = SEP_MAP[self.config["fieldSeparator"]]
                    self.config["dropCols"] = self.cols_to_drop
        except FileNotFoundError:
            print("JSON config: {} was not found".format(self.config_file))
        except json.decoder.JSONDecodeError:
            print("JSON config: {} could not be understood".format(self.config_file))
