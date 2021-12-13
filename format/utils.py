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
