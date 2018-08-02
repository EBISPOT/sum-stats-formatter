#!/bin/bash

# file needs to be chr_{chrom}_{ss_file}
filename=$1
chromosome=$2

split_files=$(ls harm_splits/${filename}/output/bpsplit_*_chr_${chromosome}.output.tsv)

# store the head in $filename
head -n 1 "harm_splits/${filename}/output/bpsplit_00_chr_${chromosome}.output.tsv" > "harm_splits/${filename}/output/merge_chr_${chromosome}.output.tsv"

for split_file in $split_files
do
    # append the data to the $filename - CHECK THAT THE ORDER IS CORRECT
    tail -n +2 $split_file >> "harm_splits/${filename}/output/merge_chr_${chromosome}.output.tsv"
    wait
    echo "Merged chromosome ${chromosome} splits into harm_splits/${filename}/output/merge_chr_${chromosome}.output.tsv"
done
