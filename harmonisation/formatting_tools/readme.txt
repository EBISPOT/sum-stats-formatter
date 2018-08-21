--------------------------------
Summary Statistics Harmonisation
--------------------------------

Within this directory, <author>_<pmid>_<study_accession>/, in addition to the 
author's files, there is the following:

harmonised/
 |- formmatted_file
 |- harmonised_file

'formatted_file' adheres to pattern: 
    <author>-<pmid>-<study_accession>-<EFO_trait>-<build>.tsv.gz

'harmonised_file' adheres to pattern: 
    <pmid>-<study_accession>-<EFO_trait>.tsv.gz  


--------------------------------------------------------------------------------

Why does this exist?

At the time of writing, there is no standard to which summary statistics file 
are made to. We are formatting and harmonising author contributed summary 
statistic files to enable users to access data with ease. 


--------------------------------------------------------------------------------

What are the formatted and harmonised files?

Formatted files:
The formatted file is the summary statistics file after it has undergone a 
semi-automated formatting process to bring it to a 'common' format. The goal is 
to enable users to access all the raw data given in the author's summary 
statistics file, but with the headers convertered to a consistent format seen 
across all formatted summary statistics files.

- Headers will be coereced to the 'common format'.
- Rows will never be removed.
- Columns may be split, merged, deleted or moved.
- Values will be unaltered.

Formatted file headings (not all may be present in file):

    'snp' = variant ID
    'pval' = p-value
    'chr' = chromosome
    'bp' = base pair location
    'or' = odds ratio
    'ci_lower' = lower 95% confidence interval
    'ci_upper' = upper 95% confidence interval
    'beta' = beta
    'se' = standard error
    'effect_allele' = effect allele
    'other_allele' = other allele
    'eaf' = effect allele frequency

Note that the headers in the formatted file are not limited to the above 
headers, nor are they required to have all of them.

Harmonised files:
The harmonised file is the output of formatted file after is has undergone an 
automated harmonisation process (repo:
https://github.com/EBISPOT/sum-stats-formatter/tree/master/harmonisation). 
The goal of the harmonisation is to make available as much of the summary 
statistics as possible in a uniform, harmonised way. This means that all 
location data will be on the same, latest genome build. Missing data that can
be inferred, will if safely possible, be inferred and provided. 
The harmonisation process is the following:

  Formatting
   1) Rename headers
   2) Set blank values to 'NA' 
  
  Mapping variant IDs to locations
   3) Update base pair location value by mapping variant ID using latest 
      Ensembl release
      OR
      if above not possible, liftover base pair location to latest genome build 
      OR
      if above not possible, remove variant from file.
  
  Harmonisation (repo: https://github.com/opentargets/sumstat_harmoniser)
   4) Using chromosome, base pair location and the effect and other alleles, 
      check the orientation of all non-palindromic variants against Ensembl VCF 
      references to detemine consensus:
      --> forward
      --> reverse
      --> mixed
      If the consensus is 'forward' or 'reverse', the following harmonisation 
      steps are performed on the palindromic variants, with the assumption that
      they are orientated according to the consensus, otherwise palindromic
      variants are not harmonised.
   5) Using chromosome, base pair location and the effect and other alleles, 
      query each variant against the Ensembl VCF reference to harmonise as
      appropriate by either:
      --> keeping record as is because:
          - it is already harmonised
          - it cannot be harmonised
      --> orientating to reference strand:
          - reverse complement the effect and other alleles
      --> flipping the effect and other alleles
          - because the effect and other alleles are flipped in the reference
          - this also means the beta, odds ratio, 95% CI and effect allele 
            frequency are inverted
      --> a combination of the orientating and flipping the alleles.
      The result of the harmonisation is the addition of a set of new fields 
      for each record (see below). A harmonisation code is assigned to each
      record indicating the harmonisation process that was performed (note
      that at the time of writing anyi processes involving 'Infer strand' are
      not being used):

     +----+--------------------------------------------------------------+
     |Code|Description of harmonisation process                          |
     +----+--------------------------------------------------------------+
     |1   |Palindromic; Infer strand; Forward strand; Alleles correct    |
     |2   |Palindromic; Infer strand; Forward strand; Flipped alleles    |
     |3   |Palindromic; Infer strand; Reverse strand; Alleles correct    |
     |4   |Palindromic; Infer strand; Reverse strand; Flipped alleles    |
     |5   |Palindromic; Assume forward strand; Alleles correct           |
     |6   |Palindromic; Assume forward strand; Flipped alleles           |
     |7   |Palindromic; Assume reverse strand; Alleles correct           |
     |8   |Palindromic; Assume reverse strand; Flipped alleles           |
     |9   |Palindromic; Drop palindromic; Not harmonised                 |
     |10  |Forward strand; Alleles correct                               |
     |11  |Forward strand; Flipped alleles                               |
     |12  |Reverse strand; Alleles correct                               |
     |13  |Reverse strand; Flipped alleles                               |
     |14  |Required fields are not known; Not harmonised                 |
     |15  |No matching variants in reference VCF; Not harmonised         |
     |16  |Multiple matching variants in reference VCF; Not harmonised   |
     |17  |Palindromic; Infer strand; EAF or reference VCF AF not known; |
     |    |Not harmonised                                                |
     |18  |Palindromic; Infer strand; EAF < specified minor allele       | 
     |    |frequency threshold; Not harmonised                           |
     +----+--------------------------------------------------------------+

  Filtering and QC
    6) Any missing variant IDs that could be found by step (5) are updated. 
    7) Records without a valid value for variant ID, chromosome, base pair
       location and p-value are removed. 

- Headers will be coerced to the 'harmonised format'.
- Addition harmonised data columns will be added.
- Rows may be removed.
- Variant ID, chromosome and base pair location may change (likely).


Harmonised file headings (not all may be present in file):

    'variant_id' = variant ID
    'p-value' = p-value
    'chromosome' = chromosome
    'base_pair_location' = base pair location
    'odds_ratio' = odds ratio
    'ci_lower' = lower 95% confidence interval
    'ci_upper' = upper 95% confidence interval
    'beta' = beta
    'standard_error' = standard error
    'effect_allele' = effect allele
    'other_allele' = other allele
    'effect_allele_frequency' = effect allele frequency
    'hm_variant_id' = harmonised variant ID
    'hm_odds_ratio' = harmonised odds ratio
    'hm_ci_lower' = harmonised lower 95% confidence interval
    'hm_ci_upper' =  harmonised lower 95% confidence interval
    'hm_beta' = harmonised beta
    'hm_effect_allele' = harmonised effect allele
    'hm_other_allele' = harmonised other allele
    'hm_effect_allele_frequency' = harmonised effect allele frequency
    'hm_code = harmonisation code (to lookup in 'Harmonisation Code Table')


