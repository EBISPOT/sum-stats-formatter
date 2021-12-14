import os
import hashlib
from format.helpers.common_constants import *

SEP_MAP = {
    'space': '\\s+',
    'tab': '\\t',
    'comma': ',',
    'pipe': '|'
}

MAND_COLS = (PVAL_DSET, CHR_DSET, BP_DSET, OR_DSET, RANGE_L_DSET, RANGE_U_DSET,
             BETA_DSET, SE_DSET, FREQ_DSET, EFFECT_DSET, OTHER_DSET, SNP_DSET)

CONFIG_SCHEMA = {
    "title": "Sumstats Formatting Schema",
    "description": "Schema for validating the summary statistics formatting configuration",
    "type": "object",
    "properties": {
        "outFilePrefix": {"type": "string"},
        "md5": {"type": "boolean"},
        "convertNegLog10Pvalue": {"type": "boolean"},
        "fieldSeparator": {"enum": [k for k in SEP_MAP.keys()]},
        "removeComments": {"type": "string",
                           "pattern": ".*"},
        "splitColumns": {"type": "array",
                         "items": {
                             "type": "object",
                             "properties": {
                                 "field": {"type": "string"},
                                 "leftName": {"type": "string"},
                                 "rightName": {"type": "string"},
                                 "separator": {"type": "string",
                                               "pattern": ".*"},
                             }
                         }
                         },
        "columnConfig": {"type": "array",
                         "items": {
                             "type": "object",
                             "properties": {
                                 "field": {"type": "string"},
                                 "rename": {"type": ["string", "null"]},
                                 "find": {"type": "string"},
                                 "replace": {"type": "string"},
                                 "extract": {"type": "string"},
                             }
                         }
                         }
    },
    "required": ["outFilePrefix",
                 "md5",
                 "convertNegLog10Pvalue",
                 "fieldSeparator",
                 "removeComments",
                 "splitColumns",
                 "columnConfig"
                 ]
}


def set_var_from_dict(dictionary, var_name, default):
    return dictionary[var_name] if var_name in dictionary else default


def env_variable_else(env_var_name, default):
    return os.environ.get(env_var_name) if os.environ.get(env_var_name) else default


def md5sum(file):
    hash_md5 = hashlib.md5()
    with open(file, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
