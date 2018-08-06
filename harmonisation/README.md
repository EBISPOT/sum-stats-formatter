# Summary Statistics harmonisation


Harmonisation scripts to run the processes outlined below.

The pipeline is managed by `snakemake`, so it can be followed in the [Snakefile](Snakefile).

Most of the harmonisation is performed by [sumstat_harmoniser](https://github.com/opentargets/sumstat_harmoniser) which is used to a) find the orientation of the variants, b) resolve RSIDs from locations and alleles and c) orientate the variants to the reference strand.


```
#pseudocode

format for sumstats database

FOR each variant in file
    IF RSID maps to genomic location in Ensembl THEN
        Update locations based on Ensembl mapping
    ELIF can liftover locations to current build THEN
        liftover locations to current build
    ELSE
        Remove variant
    ENDIF
ENDFOR

FOR each non-palindromic variant
    check orientation (query Ensembl reference VCF with chr:bp, effect and other alleles)
ENDFOR

summarise the orientation of the variants: outcomes are ‘forward’, ‘reverse’ or ‘mixed’

IF ‘mixed’ THEN
    remove palindromic variants
ELSE
    proceed with all variants (including palindromic snps) assuming consensus orientation
ENDIF

FOR each remaining variant:
    get rsid and update variant_id
    check orientation of variant against Ensembl reference VCF
    orientate variant to reference (can flip alleles, betas, ORs, CIs allele frequencies)
    remove if variant_id, p_value, base_pair_location, chromosome are invalid
ENDFOR
```
