# Summary Statistics harmonisation


Harmonisation scripts to run the processes outlined below.

The pipeline is managed by `snakemake`, so it can be followed in the [Snakefile](Snakefile).

Most of the harmonisation is performed by [sumstat_harmoniser](https://github.com/opentargets/sumstat_harmoniser) which is used to a) find the orientation of the variants, b) resolve RSIDs from locations and alleles and c) orientate the variants to the reference strand.

# Running the pipeline

The following are required:

- python3
- [HTSlib](http://www.htslib.org/download/)

python libraries:

- [snakemake](https://snakemake.readthedocs.io/en/stable/getting_started/installation.html)
- h5py
- requests
- [pyliftover](https://pypi.org/project/pyliftover/)

# Install the harmonisation scripts
`cd harmonisation`
`pip install .`

Follow the Ensembl Perl API installation instructions [here](https://www.ensembl.org/info/docs/api/api_installation.html).
If you don't have access to a mirror of the Ensembl Homo sapiens core and variation databases, you will need to [build your own](https://www.ensembl.org/info/docs/webcode/mirror/install/ensembl-data.html). If it is not possible to connect to a mirror, the RSID --> location mapping will all be done using liftover, so if the location data are not present in your file, those records will be lost.
Once you have your mirror of the Ensembl Homo sapiens core and variation databases you need to update the [registry file](https://github.com/EBISPOT/sum-stats-formatter/blob/master/harmonisation/formatting_tools/ensembl.registry) with the appropriate information.

The following directories will need to be made at the same level as the Snakefile:

```
.
├── build_38
├── formatted
├── harmonised
├── harm_splits
├── qc_harmonised
├── reports
├── toformat
└── vcf_refs
```

Any files in `toformat/` will be taken through the pipeline with the commands below.
The genome build must be given as the last 2 characters of the file name. For example if the build is NCBI36 (hg18) the build should be given as '36' in the filename like so: `<summary_statistics_file>-build36.tsv`. 

To run the pipeline simply run `snakemake` but be warned that this is not very optimal.
To run on the cluster, ssh in, clone this repository and submodules and install dependencies (above) and Ensembl data.
- Run `snakemake -j {number of jobs} -w {wait time} --cluster 'bsub {options}' &` to run it on the background. 
- To run 100 jobs in parallel do the following for example:
`snakemake -j 500 --resources load=240 -w 1800 --cluster 'bsub -o stdin.txt -e stderr' &`
The wait time limit of 1800 seconds should provide ample time to wait for output files to be generated but it should be extended/shortened as required. The `-j` flag sets the number of jobs to submit to the cluster at any one time. `--resources load=240` is important because it restricts the number of parallel jobs for any rule with `resources` defined. In our case, we want to run as many jobs in parallel as we can for all rules except for the Ensembl mapping, which makes connections to our local Ensembl database. Having the resources defined like this means we can only make 240 concurrent connections.

The pipeline will produce many outputs for the different stages, ending when there are files of the same names as all those in `toformat/` in `qc_harmonised/`. Reports are written for each file to the `reports/`, where the processes made to the file can be viewed.


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
