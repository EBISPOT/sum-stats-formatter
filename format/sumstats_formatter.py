import argparse
import os
import pandas as pd
from bsub import bsub
import sys
import format.helpers.peek as sspk
from format.utils import SEP_MAP, md5sum, set_var_from_dict, env_variable_else
import format.sumstats_table as sumstats_table
import format.sumstats_table_config as sumstats_config


def generate_config_template(table, config_name, config_type):
    table.partial_df()  # initialise heead of table
    columns_in = table.get_fields()
    sep = table.get_sep()
    config = sumstats_config.Config(columns_in=columns_in,
                                    config_file=config_name,
                                    config_type=config_type,
                                    field_sep=sep)
    config.generate_config_template()


def parse_config(config_name, config_type):
    config = sumstats_config.Config(config_file=config_name, config_type=config_type)
    config.parse_config()
    return config.config


def table_iterator(file, field_sep='\s+', remove_starting=None):
    if remove_starting:
        return pd.read_csv(file,
                           comment=remove_starting,
                           sep=field_sep,
                           dtype=str,
                           error_bad_lines=False,
                           warn_bad_lines=True,
                           engine='python',
                           chunksize=1000000)
    else:
        return pd.read_csv(file,
                           sep=field_sep,
                           dtype=str,
                           error_bad_lines=False,
                           warn_bad_lines=True,
                           engine='python',
                           chunksize=1000000)


def apply_config_to_file(file, config, preview=False):
    if preview is True:
        table = sumstats_table.Table(file,
                                     outfile_prefix=config["outFilePrefix"],
                                     field_sep=config["fieldSeparator"],
                                     remove_starting=config["removeComments"])
        table.partial_df()
        process_sumstats_table(table, config)
        table.to_csv(preview)
        if config['md5'] is True:
            md5 = md5sum(table.outfile_name)
            md5_outfile = table.outfile_name + '.md5'
            with open(md5_outfile, 'w') as f:
                f.write(md5)
        print("-------------- File out --------------")
        print(sspk.peek(table.outfile_name))
    else:
        chunks = table_iterator(file,
                                field_sep=config["fieldSeparator"],
                                remove_starting=config["removeComments"])
        header = True
        outfile_name = None
        for chunk in chunks:
            table = sumstats_table.Table(file=file,
                                         dataframe=chunk,
                                         outfile_prefix=config["outFilePrefix"],
                                         field_sep=config["fieldSeparator"],
                                         remove_starting=config["removeComments"])
            process_sumstats_table(table, config)
            table.to_csv(preview, header)
            header = False
            outfile_name = table.outfile_name
        if config['md5'] is True and outfile_name is not None:
            md5 = md5sum(outfile_name)
            md5_outfile = outfile_name + '.md5'
            with open(md5_outfile, 'w') as f:
                f.write(md5)
        print("-------------- File out --------------")
        print(sspk.peek(outfile_name))


def process_sumstats_table(table, config):
    table.field_names.extend(table.get_header())
    # drop unwanted cols
    table.drop_cols(config["dropCols"])

    # check for splits request
    splits = set_var_from_dict(config, 'splitColumns', None)
    if splits:
        if table.check_split_name_clashes(splits):
            table.perform_splits(splits)

    # find and replace
    find_replace = []
    column_config = set_var_from_dict(config, 'columnsumstats_config.Config', None)
    for field in column_config:
        if "find" and "replace" in field:
            find_replace.append(field)
    if find_replace:
        table.perform_find_replacements(find_replace)

    # extract
    extract = []
    column_config = set_var_from_dict(config, 'columnsumstats_config.Config', None)
    for field in column_config:
        if "extract" in field:
            extract.append(field)
    if extract:
        table.perform_extract(extract)

    # rename columns
    header_rename = {}
    for field in column_config:
        if "rename" in field:
            if field["rename"]:
                header_rename[field["field"]] = field["rename"]
    if header_rename:
        table.perform_header_rename(header_rename)

    # keep cols
    keep_cols = []
    for field in column_config:
        field_name = field["rename"] if "rename" in field and len(field["rename"]) > 0 else field["field"]
        keep_cols.append(field_name)
    if keep_cols:
        table.perform_keep_cols(keep_cols)
    
    if all([config['convertNegLog10Pvalue'], 'p_value' in keep_cols]):
        print('converting pvals')
        table.convert_neg_log10_pvalues()
        

def apply_config_to_file_use_cluster(file, config):
    sub = bsub("gwas_ss_format", M="3600", R="rusage[mem=3600]", N="")
    command = "tabman -f {} -config {} -mode apply".format(file, config)
    print(">>>> Submitting job to cluster, job id below")
    print(sub(command).job_id)
    print("You will receive an email when the job is finished. Formatted files, md5sums and configs will appear in "
          "the same directory as the input file.")


def check_args(args):
    config_ext = args.config.split(".")[-1]
    if args.config_type != config_ext:
        print("Please give your -config file the extension and try again: '.{}'".format(args.config_type))
        sys.exit()


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='Path to the file to be processed',
                           nargs='+',
                           required=True)
    argparser.add_argument('-sep',
                           help='The seperator/delimiter of the input file used to seperate the fields',
                           default='tab',
                           choices=['space', 'tab', 'comma', 'pipe'],
                           required=False)
    argparser.add_argument('-preview',
                           help='Show a preview (top 10 lines) of the input/output file(s)',
                           action='store_true',
                           required=False)
    argparser.add_argument('-config',
                           help='Name configuration file. You can set a path to the configuration file directory in '
                                'the environment variable SS_FORMAT_CONFIG_DIR',
                           required=False)
    argparser.add_argument('-config_type',
                           help='Type of configuration file',
                           default='xlsx',
                           choices=['xlsx', 'json'],
                           required=False)
    argparser.add_argument('-mode',
                           help='"gen" to generate the configuration file, "apply" to apply the configuration file',
                           choices=['gen', 'apply', 'apply-cluster'],
                           required=False)
    args = argparser.parse_args()

    sep = SEP_MAP[args.sep]
    if not args.mode:
        for f in args.f:
            print("Using field separator: {}".format(sep))
            print("-------------- File preview--------------")
            print(sspk.peek(f, sep=sep))
            print("Please provide some other argunents if you want to format a file")
    elif args.mode:
        if not args.config:
            print("Please provide a config file with '-config'")
            print("If using `-mode gen` just provide a name e.g. 'myconf.xlsx' for the config template to be saved as.")
            print("If using `-mode apply` or`-mode apply-cluster` please provide the name e.g. 'myconf.xlsx' of the "
                  "filled out template.")
            argparser.print_help()
            sys.exit()
        else:
            check_args(args)
            config_path = os.path.join(env_variable_else('SS_FORMAT_CONFIG_DIR', './'), args.config)
            if args.mode == 'gen':
                if len(args.f) != 1:
                    print("You can only specify one file for the template generation")
                    sys.exit()
                file = args.f[0]
                print("Using field separator: {}".format(sep))
                table = sumstats_table.Table(file, field_sep=sep)
                print("Generating configuration template...")
                generate_config_template(table, config_path, args.config_type)
            elif args.mode == 'apply':
                print("Parsing config...")
                config_dict = parse_config(config_path, args.config_type)
                print("Applying configuration...")
                for f in args.f:
                    print(f)
                    apply_config_to_file(f, config_dict, args.preview)
            elif args.mode == 'apply-cluster':
                print("Applying configuration using cluster job...")                
                for f in args.f:
                    apply_config_to_file_use_cluster(f, config_path)
    else:
        print("Please provide some argunents")
        argparser.print_help()


if __name__ == '__main__':
    main()
