import argparse
import os
import pandas as pd
import subprocess
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
    column_config = set_var_from_dict(config, 'columnConfig', None)
    for field in column_config:
        if "find" and "replace" in field:
            find_replace.append(field)
    if find_replace:
        table.perform_find_replacements(find_replace)

    # extract
    extract = []
    column_config = set_var_from_dict(config, 'columnConfig', None)
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


def apply_config_to_file_use_cluster(file_, config_type, config_path, memory):
    # Define output and error file paths
    output_file = "slurm-%j.out"  # %j will be replaced with the job ID
    error_file = "slurm-%j.err"  # %j will be replaced with the job ID

    # SLURM job submission command
    sbatch_command = [
        "sbatch", 
        f"--mem={memory}", 
        "--time=01:00:00", 
        "--output='/homes/karatugo/sumstatsformatter/apply-cluster.out'", 
        "--error='/homes/karatugo/sumstatsformatter/apply-cluster.err'", 
        "--wrap",
        f"ss-format -f {file_} -t {config_type} -c {config_path} -m apply"
    ]

    print(">>>> Submitting job to SLURM, job id below")

    # Executing the sbatch command
    result = subprocess.run(sbatch_command, capture_output=True, text=True)

    if result.returncode != 0:
        print("Error in submitting job: ", result.stderr)
        return

    print("Command output: ", result.stdout)

    try:
        job_id = result.stdout.strip().split()[-1]
        print("Job ID: ", job_id)
    except IndexError as e:
        print("Error parsing job ID: ", e)
        print("Full output: ", result.stdout)


    print(
        "You will receive an email when the job is finished. Formatted files, md5sums and configs will appear in "
        "the same directory as the input file."
    )



def check_args(args):
    config_ext = args.config_name.split(".")[-1]
    if args.config_type != config_ext:
        print("Please give your --config_name file the extension and try again: '.{}'".format(args.config_type))
        sys.exit()


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', '--filepath',
                           help='Path to the file to be processed',
                           nargs='+',
                           required=True)
    argparser.add_argument('-s', '--separator',
                           help='The separator/delimiter of the input file used to separate the fields',
                           default='tab',
                           choices=['space', 'tab', 'comma', 'pipe'],
                           required=False)
    argparser.add_argument('-p', '--preview',
                           help='Show a preview (top 10 lines) of the input/output file(s)',
                           action='store_true',
                           required=False)
    argparser.add_argument('-c', '--config_name',
                           help='Name configuration file. You can set a path to the configuration file directory in '
                                'the environment variable SS_FORMAT_CONFIG_DIR',
                           required=False)
    argparser.add_argument('-t', '--config_type',
                           help='Type of configuration file, default: "json"',
                           default='json',
                           choices=['json', 'xlsx'],
                           required=False)
    argparser.add_argument('-m', '--mode',
                           help='"gen" to generate the configuration file, "apply" to apply the configuration file',
                           choices=['gen', 'apply', 'apply-cluster'],
                           required=False)
    argparser.add_argument('-M', '--cluster_mem',
                           help='Option to specify cluster memory in MB when using `--mode apply-cluster`, \
                           default: "3600"',
                           default='3600',
                           required=False)

    args = argparser.parse_args()

    sep = SEP_MAP[args.separator]
    if not args.mode:
        for f in args.filepath:
            print("Using field separator: {}".format(sep))
            print("-------------- File preview--------------")
            print(sspk.peek(f, sep=sep))
            print("Please provide some other argunents if you want to format a file")
    elif args.mode:
        if not args.config_name:
            print("Please provide a config file with '--config_name'")
            print("If using `--mode gen` just provide a name e.g. \
            'myconf.xlsx' for the config template to be saved as.")
            print("If using `--mode apply` or`--mode apply-cluster` please provide the name e.g. 'myconf.xlsx' of the "
                  "filled out template.")
            argparser.print_help()
            sys.exit()
        else:
            check_args(args)
            config_path = os.path.join(env_variable_else('SS_FORMAT_CONFIG_DIR', './'), args.config_name)
            if args.mode == 'gen':
                if len(args.filepath) != 1:
                    print("You can only specify one file for the template generation")
                    sys.exit()
                file = args.filepath[0]
                print("Using field separator: {}".format(sep))
                table = sumstats_table.Table(file, field_sep=sep)
                print("Generating configuration template...")
                generate_config_template(table, config_path, args.config_type)
            elif args.mode == 'apply':
                print("Parsing config...")
                config_dict = parse_config(config_path, args.config_type)
                print("Applying configuration...")
                for f in args.filepath:
                    print(f)
                    apply_config_to_file(f, config_dict, args.preview)
            elif args.mode == 'apply-cluster':
                # TODO: update the code here
                print("Applying configuration using cluster job...")                
                for f in args.filepath:
                    apply_config_to_file_use_cluster(
                        f, 
                        config_type=args.config_type,
                        config_path=config_path,
                        memory=args.cluster_mem,
                    )
    else:
        print("Please provide some arguments")
        argparser.print_help()


if __name__ == '__main__':
    main()
