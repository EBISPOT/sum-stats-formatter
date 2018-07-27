#!/bin/bash

# file needs to be {ss_file}
filename=$1

#split_files=merge_chr_{1..22}_$filename

# store the 
head -n 1 "merge_chr_1_${filename}" > "merge_${filename}"
for split_file in merge_chr_{1..22}_$filename
do
    # append the data to the $filename - CHECK THAT THE ORDER IS CORRECT
    if [ -e $split_file ]
    then
        tail -n +2 $split_file >> "merge_${filename}"
    else
        echo "${split_file} doesn't exist"
    fi
done
