#!/bin/bash

# file needs to be {ss_file}
filename=$1

#split_files=merge_chr_{1..22}_$filename

# store the 
head -n 1 "harm_splits/${filename}/output/merge_chr_1.output.tsv" > "harm_splits/${filename}/output/final.output.tsv"

for split_file in harm_splits/$filename/output/merge_chr_*.output.tsv
do
    # append the data to the $filename - CHECK THAT THE ORDER IS CORRECT
    if [ -e $split_file ]
    then
        tail -n +2 $split_file >> "harm_splits/${filename}/output/final.output.tsv"
    else
        echo "${split_file} doesn't exist"
    fi
done
wait
echo "file written to harm_splits/${filename}/output/final.output.tsv"
cp "harm_splits/${filename}/output/final.output.tsv" "harmonised/${filename}.tsv"
wait
echo "file copied to harmonised/${filename}.tsv"
