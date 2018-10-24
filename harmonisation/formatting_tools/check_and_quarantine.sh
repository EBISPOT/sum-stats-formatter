#!/bin/bash

new_studies_dir=$1
quarantine_dir=$2
success_dir=$3

for f in ${new_studies_dir}*; do

	filename=$(basename $f)
        filename_noext=$(echo $filename | cut -f1 -d '.')
	log_file=${quarantine_dir}/${filename_noext}.log        

	echo "Checking $filename"
	
	# if the file is a file
        if [ -f $f ]; then
		python check_file_name.py --filename $f -o $log_file
		if [ -s $log_file ] && grep -q 'ERROR' $log_file; then
			echo "Moving $f to $quarantine_dir"
			mv $f $quarantine_dir
		else
			python pre_checks.py -f $f -o $log_file
			if [ -s $log_file ] && grep -q 'ERROR' $log_file; then
				echo "Moving $f to $quarantine_dir"
				mv $f $quarantine_dir
			else
			        echo "Moving $f to $success_dir"	
				mv $f $success_dir
			fi
		fi
		
        else
		echo "skipping $filename because it isn't a regular file"
	fi
done
