# Summary Statistics Formatter

A general purpose formatter for summary statistics raw files.

The sum stats formatter's goal is to give a common format to the raw summary statistics file of interest.

It reads the header file and takes a first guess at what each column is. If you look into the [utils file](https://github.com/EBISPOT/SumStats/blob/master/sum-stats-formatter/format/utils.py) you can see 
the guesses that it will make and the format we want to give to the sum stats file.

# Installation
- clone the repository
    - git clone https://github.com/EBISPOT/SumStats.git
    - cd SumStats
- cd sum-stats-formatter
- pip install .

This will install the formatter on your python path and you should be able to use the scripts via command line.

Assuming you have python and pip installed.

# Quick summary of proposed steps
Tip: for each command you can use the `-h` option for it to tell you the arguments it needs.

1. Create a directory and open a terminal under that directory
2. Unzip/untar/decompress the original file that you want
3. `peek` in to see what it looks like
4. Run `format -f <full path to orginal file>`. This will guess the format and save the file in your directory, named `formatted_<filename>`.
5. Read the output of the above command on your terminal. It will tell you what guesses it made to the headers, and also give you a peek into the formatted file.
6. Make any extra modifications that you need to the headers. Don't forget to use the formatted file now as an input (if it guessed decently)
7. Use `valid-headers` to see the default header names that we want
8. When done run `rename-file` and `compress` to rename the file and compress it (use the renamed file for compression)
9. You can delete any files that you don't need now. 

# Guideline
The file given as an input should be a tab, comma, space separated file with headers for each column.

We propose that you create a directory somewhere and open the terminal to run the script there. The original file can be anywhere, but if you run the script under your newly created directory, the formatted file will be saved there.

We propose that you first run the `format` command (see below), as it will try and guess what each header is (where possible) and rename it to a common format. It will also try to separate any chr_position combinations, and remove the prefix `chr` or `chr_` from the chromosome data where applicable.

The output of the `format` command will be saved in a tab separated file named `formatted_<filename>.tsv` where filename is the name of the original file. It will also print on the screen the changes it made in the headers, so you can further edit the ones you disagree on, or discard the whole file.

You can continue with further changes on headers and data columns by using the commands listed in the section below.

At any time you can use the commad `peek` that will print the header and the first line of data, so you know what a file looks like whitout having to open it.

# Available commands

You can see all available actions by typing:

`$ help-ss`

on your terminal. It will show you the range of commands you can execute on the file and some information about each one.

The actions you can perform (on a summary statistics file, i.e. csv, tsv, or other) are:

- `$ peek -f <filename>` 
   - will give you a peak into the file of interest displaying it's header and the first row of data
- `$ valid-headers` 
   -  will print a list of all known valid headers for inspiration
- `$ format -f <filename>` 
   - will try to automatically format the file
   - the new formatted file will by named `formatted_<filename>.tsv` and it will be created under the working directory
- `$ rename -f <filename>' -old <old name> -new <new name>` 
   - will rename the header given in the paramater '-old' to the header in the parameter '-new'
- `$ merge -f <filename>' : -left <first header> -right <second header> -d <delimiter> -new <new header>` 
   - will merge the columns of left header and right header into one column using the delimiter provided, 
   the new column header will be the one specified with the parameter '-new'
- `$ clean -f <filename>' : -header <header> -fixture <fixture>` 
   - will remove the sequence given as a fixture from the data in the column specified 
- `$ split -f <filename>' -header <header> -left <left header> -right <right header> -d <delimiter>` 
   - will split the column specified in '-header' using the delimiter provided
   - the split columns will be renamed using the parameters '-left' for the value on the left (after the split), 
   and '-right' for the value on the right (after the split)
- `$ swap -f <filename>' -left <left header> -right <right header>` 
   - will swap the data of left header and right header
- '$ allele-swap -f <filename>' -header <header>
   - will swap the effect allele and other allele when the value in specified column is FALSE
- `$ delete -f <filename>' -headers <list of comma separated headers>`
   - will remove the columns of the specified headers from the file
- `$ rename-file -f <filename>\' -efo <efo trait> -study <study accession> -b <genome build> -pmid <pmid> -author <author>`
   - will rename file as: author_pmid_studyaccession_efotrait_genomebuild.tsv
- `$ compress -f <filename>'` 
   - will compress the file into a .tar.gz archive 
   
You can type `$ <command> -h` to get the argument explanation for each command. E.g.: `$ merge -h`

With this suite of commands you can semi-automate the formatting and then go in and perform any other 
renaming/merging/swapping/cleaning/splitting on the data columns.

NOTE: You will usually want to use the original file and first format the raw data, and then for any other commands that you will use,
you will provide as input (-f <filename>) the generated formatted file, named: `formatted_<filename>.tsv`.
   
# Example
### original file: filename: Study1.csv
### original file header and first row of data
```--
assay.name : rs12736689
scaffold : chr1
position : 182549729
alleles : C/T
effect : -0.298
stderr : 0.0346
pvalue : 6.97e-18
gt.rate : NA
freq.b : NA
avg.rsqr : 0.971
dose.b : 0.974
```
### formatting the original file
Run:

`$ format -f Study1.csv`

```--
------> Output saved in file: formatted_Study1.tsv <------

Please use this file for any further formatting.

Showing how the headers where mapped below...

assay.name  ->  snp
scaffold  ->  chr
position  ->  bp
effect  ->  beta
stderr  ->  se
pvalue  ->  pval

Peeking into the new file...

snp : rs12736689
chr : 1
bp : 182549729
alleles : C/T
beta : -0.298
se : 0.0346
pval : 6.97e-18
gt_rate : NA
freq_b : NA
avg_rsqr : 0.971
dose_b : 0.974
```
### splitting up the alleles to effect_allele and other_allele
NOTE: I am using as an input the file that was just created, not the original file

`$ split -f formatted_Study1.tsv -header alleles -left effect_allele -right minor_allele -d /`

`------> Split data saved in: formatted_Study1.tsv <------`

Changes applied on the same formatted file.
### checking to see if we are satisfied with the result
`$ peek -f formatted_Study1.tsv`

```--
------> Peeking into file: formatted_Study1.tsv <------

snp : rs12736689
chr : 1
bp : 182549729
beta : -0.298
se : 0.0346
pval : 6.97e-18
gt_rate : NA
freq_b : NA
avg_rsqr : 0.971
dose_b : 0.974
effect_allele : C
minor_allele : T
```
### renaming freq_b to eaf
`$ rename -f formatted_Study1.tsv -old freq_b -new eaf`

`------> Renamed data saved in: formatted_Study1.tsv <------`
### checking to see if we are satisfied with the result
`$ peek -f formatted_Study1.tsv`

```--
------> Peeking into file: formatted_Study1.tsv <------
snp : rs12736689
chr : 1
bp : 182549729
beta : -0.298
se : 0.0346
pval : 6.97e-18
gt_rate : NA
eaf : NA
avg_rsqr : 0.971
dose_b : 0.974
effect_allele : C
minor_allele : T
```
### renaming the filename
`$ rename-file -f formatted_Study1.tsv -efo EFO_00001 -study GCST0000001 -b 38 -pmid 123456 -author AuthorName`

`------> File renamed as: AuthorName_123456_GCST0000001_EFO_00001_38.tsv <------`
### compressing the file, it's ready!
`$ compress -f AuthorName_123456_GCST0000001_EFO_00001_38.tsv`

`------> Compressing file: AuthorName_123456_GCST0000001_EFO_00001_38.tsv <------`

`------> Compressed as: AuthorName_123456_GCST0000001_EFO_00001_38.tar.gz <------`
