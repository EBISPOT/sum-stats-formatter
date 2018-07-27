#!/bin/bash

file=$1
filename=$(basename $file)
number=16

tail -n +2 $file > "data_${filename}" 
split --number=l/$number -d --additional-suffix=_$filename "data_${filename}" bpsplit_
for split_file in bpsplit_*
do
	head -n 1 $file > "head_${split_file}"
	cat $split_file >> "head_${split_file}"
	mv -f "head_${split_file}" $split_file
done
rm "data_${filename}"
