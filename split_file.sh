#!/bin/bash

file=$1
filename=$(basename $file)
number=$2


tail -n +2 $file > "data_${filename}" 
split --number=l/$number -d --additional-suffix=_$filename "data_${filename}" split_
split_files=$(ls split_*_${filename})
for split_file in $split_files
do
	head -n 1 $file > "head_${split_file}"

	while [ -f $split_file ];
	do
		echo $split_file
		cat "head_${split_file}" $split_file > "bp${split_file}"
	rm $split_file
	rm "head_${split_file}"
	done

done
rm "data_${filename}"
