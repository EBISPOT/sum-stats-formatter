import argparse
import csv
import os
import subprocess
import logging
from pyliftover.chainfile import open_liftover_chain_file
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as pf
from pyspark.sql.types import *
import glow

spark = SparkSession.builder.appName("snp_mapper").config("spark.jars.packages", "io.projectglow:glow_2.11:0.3.0").getOrCreate()
glow.register(spark)

from formatting_tools.utils import *
from formatting_tools.liftover import *

logger = logging.getLogger('update_locations')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')


VCF_SCHEMA = StructType([
    StructField("CHROM", IntegerType(), False),
    StructField("POS", IntegerType(), False),
    StructField("ID", StringType(), False),
    StructField("REF", StringType(), False),
    StructField("ALT", StringType(), False),
    StructField("QUAL", StringType(), False),
    StructField("FILTER", StringType(), False),
    StructField("INFO", StringType(), False)
    ])


def open_process_file(file, vcf_ref, out_dir, from_build, to_build, chromosomes):
    # Source the chain file
    from_db = ucsc_release.get(from_build)
    to_db = ucsc_release.get(to_build)
    f = open_liftover_chain_file(from_db=from_db, to_db=to_db, search_dir=".", cache_dir=".", use_web=True, write_cache=True)
    chain_file = f.name
    f.close()
    
    # Load the VCF 
    filename = file.split("/")[-1].split(".")[0]
    vcf_df = spark.read.csv(vcf_ref, sep="\t", comment="#", schema=VCF_SCHEMA).select("POS", "ID")

    # Load the sumstats
    ssdf = spark.read.csv(file, sep="\t", header=True)
    
    # Map the sumstats SNPs to position in VCF
    ssdf = ssdf.join(vcf_df, (pf.col("ID") == pf.col(SNP_DSET)), how="left")
    mapped_df = ssdf.filter(ssdf["ID"].isNotNull())
    
    # Filter the unmapped ones, 
    unmapped_df = ssdf.filter(ssdf["ID"].isNull()).withColumn("end", 1 + ssdf[BP_DSET])
    unmapped_df = unmapped_df.withColumn("chrom", pf.concat(pf.lit("chr"), ssdf[CHR_DSET].cast("string")))
    unmapped_df = unmapped_df.withColumn("chrom", pf.when(ssdf[CHR_DSET] == 23, "chrX").when(ssdf[CHR_DSET] == 24, "chrY").otherwise(unmapped_df.chrom))
    liftover_expr = "lift_over_coordinates(chrom, base_pair_location, end, '{chain_file}', .95)".format(chain_file=chain_file)
    lifted_df = unmapped_df.withColumn('lifted', pf.expr(liftover_expr))
    lifted_df = lifted_df.withColumn("POS", pf.col('lifted.start')).drop("lifted", "end", "chrom")

    out_df = mapped_df.union(lifted_df)
    out_df = out_df.withColumn(BP_DSET, out_df["POS"]).drop("POS", "ID")
    
    outfile = os.path.join(out_dir, filename)
    out_df.toPandas().to_csv(outfile, sep="\t", header=True)

    
def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-f', help='The full path to the file to be processed', required=True)
    argparser.add_argument('-vcf_ref', help='The full path to the vcf reference file', required=True)
    argparser.add_argument('-d', help='The directory to write to', required=True)
    argparser.add_argument('-from_build', help='The original build e.g. "36" for NCBI36 or hg18', required=True)
    argparser.add_argument('-to_build', help='The latest (desired) build e.g. "38"', required=True)
    argparser.add_argument('--log', help='path to the log file')
    argparser.add_argument('-config', help='path to the config.yaml file')
    args = argparser.parse_args()

    file = args.f
    vcf_ref = args.vcf_ref
    out_dir = args.d
    from_build = args.from_build
    to_build = args.to_build
    log_file = args.log
    config_file = args.config

    chromosomes = get_chromosome_list(config_file)

    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    open_process_file(file, vcf_ref, out_dir, from_build, to_build, chromosomes)


if __name__ == "__main__":
    main()
