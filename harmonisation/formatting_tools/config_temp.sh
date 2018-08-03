#!/usr/bin/env bash


# Create index of the vcf
#bgzip -c reference_chr1_vcf.testdata.vcf > reference_chr1_vcf.testdata.vcf.gz
#tabix -p vcf gnomad.genomes.r2.0.2.sites.chr1.vcf.gz


# Run harmoniser
mkdir -p output
python sumstat_harmoniser.py --sumstats bp_{step}.{ss_name}.tsv \
  --vcf gnomad.genomes.r2.0.2.sites.chr{chromosome}.vcf.bgz \
  --out output/bp_{step}_{ss_name}.output.tsv \
  --log output/bp_{step}_{ss_name}.log.tsv.gz \
  --rsid_col variant_id \
  --chrom_col chromosome \
  --pos_col base_pair_location \
  --effAl_col effect_allele \
  --otherAl_col other_allele \
  --eaf_col ma_freq \
  --beta_col beta \
  --maf_palin_threshold 0.42 \
  --af_vcf_field AF \
  --af_vcf_min 0.001 \
  --infer_strand True \
  --infer_palin True \
  --chrom {chromosome} \
  --bp_start {bp_start} \
  --bp_end {bp_end} 
