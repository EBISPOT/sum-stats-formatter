import pandas as pd
import pathlib
import os
import argparse



def get_extension(file):
    file_extension = ""
    extensions = pathlib.Path(file).suffixes
    common_exts = [".tsv", ".csv", ".psv", ".txt"]
    if len(extensions) == 1:
        file_extension = extensions[0]
    elif len(extensions) > 1:
        ext = [i for i in common_exts if i in extensions]
        if len(ext) == 1:
            i = extensions.index(ext[0])
            file_extension = "".join(extensions[i:])
        else:
            file_extension = extensions[-1]
    return file_extension


def get_filename(file, file_extension):
    get_extension(file)
    filename = pathlib.Path(file).name.replace(file_extension, '')
    return filename

def get_parent_dir(file):
    return pathlib.Path(file).parent



def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='Path to the file to be processed', required=True)
    args = argparser.parse_args()
    
    df = pd.read_csv(args.f, sep='\t')
    header_rename = {"VARIANT_ID": "variant_id", 
                    "chromosome": "chromosome",
                    "position": "base_pair_location",
                    "Allele1": "effect_allele",
                    "Allele2": "other_allele",
                    "Effect": "beta",
                    "StdErr": "standard_error",
                    "log(P)": "log(P)"}
    df["p_value"] = (10 ** df["log(P)"])
    df = df.rename(columns=header_rename)
    df['variant_id'] = df['variant_id'].str.extract(r"(rs[0-9]+)")

    ext = get_extension(args.f)
    filename = get_filename(args.f, ext)
    par_dir = get_parent_dir(args.f)

    outfile = os.path.join(par_dir, "formatted_" + filename + ".tsv")
    df.to_csv(outfile,
                mode='w',
                header=True,
                sep="\t",
                na_rep="NA",
                index=False)
    

if __name__ == '__main__':
    main()
