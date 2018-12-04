#!/bin/bash

# Script to find the summary statistics directory on the FTP based on study
# accession.
# If the directory exists, the harmonised files will be copied there, otherwise
# the directory will be made first.


# ARGS: (1) The FTP path for the summary statistics files
#       (2) The study accesion
#	(3) The files to copy


ftp_path=$1
study=$2
formatted_dir=$3
harmonised_dir=$4
readme=$5

ss_path=$(find $ftp_path -type d -name *_$study)
formatted_file_path=$(find $formatted_dir -type f -name "*$study*")
harmonised_file_path=$(find $harmonised_dir -type f -name "*$study*")

formatted_file=$(basename $formatted_file_path .tsv)
harmonised_file=$(basename $harmonised_file_path .tsv)


# If no path with study accession:

if [ ! -d "$ss_path" ]; then
	echo "No directory for $study"
#	echo "Checking database to try and generate..."
#        ss_dir=$(python2 /nfs/spot/data/test/sumstats/formatting_tools/generate_sumstats_dir_name.py --study $study)
#	echo $ss_dir

#	if [[ $ss_dir =~ "ERROR" ]]; then
#		echo "ERROR: Could not find ${study} in the database" | mail -s "File failed to copy" jhayhurst@ebi.ac.uk
#	else
#		echo "making dir: ${ftp_path}${ss_dir}"
#		mkdir $ftp_path$ss_dir
#		ss_path=$ftp_path$ss_dir
#	fi
else
	# If path with study accession, get the name of that directory:
	ss_dirname=$(echo $ss_path | rev | cut -f1 -d "/" | rev)
	echo "$ss_dirname exists" 
fi

if [ -d "$ss_path" ]; then
	# if harmonised files don't exist:
	mkdir -p $ss_path/harmonised
        #rm -vf $ss_path/harmonised/*.tsv.f.gz
        #rm -vf $ss_path/harmonised/*.tsv.h.gz
        if [ ! -e "${ss_path}/harmonised/${formatted_file}.f.tsv.gz" ]; then
		gzip < $formatted_file_path > $ss_path/harmonised/$formatted_file.f.tsv.gz
		echo "copying: $formatted_file_path to $ss_path/harmonised/"
	else
		echo "$formatted_file_path already exists"
	fi
        if [ ! -e "$ss_path/harmonised/${harmonised_file}.h.tsv.gz" ]; then
                gzip < $harmonised_file_path > $ss_path/harmonised/$harmonised_file.h.tsv.gz
                echo "copying: $harmonised_file_path to $ss_path/harmonised/"
        else
                echo "$harmonised_file_path already exists"
        fi
	if [ ! -e "${ss_path}/harmonised/readme.txt" ]; then
		cp $readme $ss_path/harmonised/
	fi
fi
