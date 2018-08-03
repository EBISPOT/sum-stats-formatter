# Configure --------------------------------------------------------------------

configfile: "config.yaml"
STUDIES, = glob_wildcards(config["ss_file_pattern"])

# --------------------------Snakemake rules ------------------------------------
# To do: parameterise directories


rule all:
    input:
        expand("harmonised/{ss_file}.tsv", ss_file=STUDIES)


# Sumstat formatting

rule sumstat_format:
    input:
        "toformat/{ss_file}.tsv"
    output:
        "formatted/{ss_file}.tsv"
    shell:
        "python formatting_tools/sumstats_formatting.py "
        "-f {input} "
        "-d formatted/"


# Liftover to current build

#rule liftover:
#    input:
#        "formatted/{ss_file}.tsv",
#        "formatting_tools/build_info.tsv"
#    output:
#        "build_38/{ss_file}.tsv"
#    params:
#        desired_build=config["desired_build"],
#        script_path=config["script_path"]
#    shell:
#        "./formatting_tools/liftover_ss_files.sh {input}"


# Resolve missing locations

#rule resolve_missing_locations:
#    input:
#        "build_38/{ss_file}.tsv"
#    output:
#        "resolved_chrbp/{ss_file}.tsv"
#    shell:
#        "python formatting_tools/resolve_location.py "
#        "-f {input}"



# Harmonisation

rule get_vcf_files:
    output:
        "vcf_refs/homo_sapiens-chr{chromosome}.vcf.gz"
    params:
        location=config["remote_vcf_location"]
    shell:
        "wget -P vcf_refs/ {params.location}/homo_sapiens-chr{wildcards.chromosome}.vcf.gz"


rule get_tbi_files:
    output:
        "vcf_refs/homo_sapiens-chr{chromosome}.vcf.gz.tbi"
    params:
        location=config["remote_vcf_location"]
    shell:
        "wget -P vcf_refs/ {params.location}/homo_sapiens-chr{wildcards.chromosome}.vcf.gz.tbi"


# split files into fractions (e.g. 16 and then again by 16)

rule split_file:
    input:
        "formatted/{ss_file}.tsv"
    output:
        expand("formatted/{{ss_file}}/bpsplit_{step}_bpsplit_{split}_{{ss_file}}.tsv", step=config["steps"], split=config["splits"])
    shell:
        "mkdir -p formatted/{wildcards.ss_file}; cp formatted/{wildcards.ss_file}.tsv formatted/{wildcards.ss_file}/{wildcards.ss_file}.tsv; "
        "./formatting_tools/split_file.sh formatted/{wildcards.ss_file}/{wildcards.ss_file}.tsv 16; "
        "for split in formatted/{wildcards.ss_file}/bpsplit*.tsv; do ./formatting_tools/split_file.sh $split 16; done"

# map rsids to chr:bp location

rule retrieve_ensembl_mapping_data:
    input:
        "formatted/{ss_file}/bpsplit_{step}_bpsplit_{split}_{ss_file}.tsv"
    output:
        "formatted/{ss_file}/bpsplit_{step}_bpsplit_{split}_{ss_file}.tsv.out"
    shell:
        "./formatting_tools/var2location.pl {input}"


rule update_locations_from_ensembl:
    input:
        "formatted/{ss_file}/bpsplit_{step}_bpsplit_{split}_{ss_file}.tsv.out",
        in_ss="formatted/{ss_file}/bpsplit_{step}_bpsplit_{split}_{ss_file}.tsv"
    output:
        "build_38/{ss_file}/bpsplit_{step}_bpsplit_{split}_{ss_file}.tsv"
    params:
        to_build=config["desired_build"],
    shell:
        "filename={wildcards.ss_file}; "
        "from_build=$(echo -n $filename | tail -c 2); " 
        "python formatting_tools/update_locations.py -f {input.in_ss} -d build_38/{wildcards.ss_file} -from_build $from_build -to_build {params.to_build}"


rule cat_all_splits:
    input:
        expand("build_38/{{ss_file}}/bpsplit_{step}_bpsplit_{split}_{{ss_file}}.tsv", step=config["steps"], split=config["splits"])
    output:
        "build_38/{ss_file}/{ss_file}.tsv"
    shell:
        "./formatting_tools/cat_splits_alt.sh {wildcards.ss_file}"


# split the file by chromosome

rule split_by_chrom:
    input:
        "build_38/{ss_file}/{ss_file, \d+-GSCT\d+-EFO_\d+}.tsv"
    output:
        "build_38/{ss_file}/chr_{chromosome}.tsv"
    shell:
        "python formatting_tools/split_by_chromosome.py -f {input} -chr {wildcards.chromosome} -d build_38/"

# collect unmapped chromosomes and add to log


# split files into fractions (e.g. 16 and then again by 16)

rule split_by_bp:
    input:
        "build_38/{ss_file, \d+-GSCT\d+-EFO_\d+}/chr_{chromosome}.tsv"
    output:
        expand("build_38/{{ss_file}}/bpsplit_{step}_chr_{{chromosome}}.tsv", step=config["steps"])
    shell:
        "./formatting_tools/split_file.sh {input} 16"


# run sumstat_harmoniser.py to get strand counts

rule generate_strand_counts:
    input:
        "vcf_refs/homo_sapiens-chr{chromosome}.vcf.gz",
        "vcf_refs/homo_sapiens-chr{chromosome}.vcf.gz.tbi",
        in_ss="build_38/{ss_file}/bpsplit_{step}_chr_{chromosome}.tsv"
    output:
        "harm_splits/{ss_file}/output/strand_count_bpsplit_{step, \d+}_chr_{chromosome}.tsv"
    shell:
        "mkdir -p harm_splits/{wildcards.ss_file}/output;"
        "./formatting_tools/sumstat_harmoniser/bin/sumstat_harmoniser --sumstats {input.in_ss} "
        "--vcf vcf_refs/homo_sapiens-chr{wildcards.chromosome}.vcf.gz "
        "--chrom_col chromosome "
        "--pos_col base_pair_location "
        "--effAl_col effect_allele "
        "--otherAl_col other_allele "
        "--strand_counts harm_splits/{wildcards.ss_file}/output/strand_count_bpsplit_{wildcards.step}_chr_{wildcards.chromosome}.tsv" 
        

# make strand counts 

rule make_strand_count:
    input:
        expand("harm_splits/{{ss_file}}/output/strand_count_bpsplit_{step}_chr_{chromosome}.tsv", step=config["steps"], chromosome=config["chromosomes"])
    output:
        "harm_splits/{ss_file}/output/total_strand_count.csv"
    shell:
        "python formatting_tools/sum_strand_counts.py -i harm_splits/{wildcards.ss_file}/output/ -o harm_splits/{wildcards.ss_file}/output/" 


# run sumstat_harmoniser.py for each split

rule run_harmonisation_per_split:
    input:
        "vcf_refs/homo_sapiens-chr{chromosome}.vcf.gz",
        "vcf_refs/homo_sapiens-chr{chromosome}.vcf.gz.tbi",
        "harm_splits/{ss_file}/output/total_strand_count.csv",
        in_ss="build_38/{ss_file}/bpsplit_{step, \d+}_chr_{chromosome}.tsv"
    output:
        "harm_splits/{ss_file}/output/bpsplit_{step, \d+}_chr_{chromosome}.output.tsv"
    shell:
        "palin_mode=$(grep palin_mode harm_splits/{wildcards.ss_file}/output/total_strand_count.csv | cut -f2 );"
        "./formatting_tools/sumstat_harmoniser/bin/sumstat_harmoniser --sumstats {input.in_ss} "
        "--vcf vcf_refs/homo_sapiens-chr{wildcards.chromosome}.vcf.gz "
        "--hm_sumstats harm_splits/{wildcards.ss_file}/output/bpsplit_{wildcards.step}_chr_{wildcards.chromosome}.output.tsv "
        "--hm_statfile harm_splits/{wildcards.ss_file}/output/bpsplit_{wildcards.step}_chr_{wildcards.chromosome}.log.tsv.gz "
        "--chrom_col chromosome "
        "--pos_col base_pair_location "
        "--effAl_col effect_allele "
        "--otherAl_col other_allele "
        "--beta_col beta "
        "--palin_mode $palin_mode"


# concatenate harmonised splits 

rule concatenate_bp_splits:
    input:
        expand("harm_splits/{{ss_file}}/output/bpsplit_{step}_chr_{{chromosome}}.output.tsv", step=config["steps"])
    output:
        "harm_splits/{ss_file}/output/merge_chr_{chromosome}.output.tsv"
    shell:
        "./formatting_tools/cat_bpsplits.sh {wildcards.ss_file} {wildcards.chromosome}"


# concatenate chromosomes into one file

rule concatenate_chr_splits:
    input:
        expand("harm_splits/{{ss_file}}/output/merge_chr_{chromosome}.output.tsv", chromosome=config["chromosomes"])
    output:
        "harmonised/{ss_file}.tsv"
    shell:
        "./formatting_tools/cat_chroms.sh {wildcards.ss_file}"

# clean up
# final QC and make X=23 and Y=24
