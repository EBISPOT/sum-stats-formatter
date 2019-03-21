import glob
import argparse
import pandas as pd



def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-dir', help='The name of the directory containing the files that need to concatenated')
    argparser.add_argument('-out', help='The name of the outfile')

    args = argparser.parse_args()

    dir = args.dir
    out = args.out
    files = glob.glob(dir + "*")

    print("Concatenating the following files:")
    for f in files:
        print(f)

    dfs = (pd.read_csv(f, sep="\s+", dtype=str) for f in files)

    df_cat = pd.concat(dfs, ignore_index=True)

    print("Writing concatenated file to {o}".format(o=out))
    df_cat.to_csv(out, sep="\t", na_rep="NA", index=False)


if __name__ == "__main__":
    main()
