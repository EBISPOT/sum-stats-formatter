#!/bin/bash

filename=$1

# store the head in $filename
head -n 1 "build_38/${filename}/bpsplit_00_bpsplit_00_${filename}.tsv" > "build_38/${filename}/${filename}.tsv"

for i in {00..99}
do
    for j in {00..99}
    do
        split="build_38/${filename}/bpsplit_${j}_bpsplit_${i}_${filename}.tsv"
        if [ -e $split ]
        then
            echo "appending $split"
            tail -n +2 $split >> "build_38/${filename}/${filename}.tsv"
            wait
            rm $split
        fi
    done
    wait
done
