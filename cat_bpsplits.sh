#!/bin/bash

# file needs to be chr_{chrom}_{ss_file}
filename=$1

split_files=$(ls bpsplit_*_${filename})

# store the head in $filename
head -n 1 "bpsplit_00_${filename}" > "merge_${filename}"
for split_file in $split_files
do
    # append the data to the $filename - CHECK THAT THE ORDER IS CORRECT
    tail -n +2 $split_file >> "merge_${filename}"
done
