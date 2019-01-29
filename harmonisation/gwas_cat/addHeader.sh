#!/bin/bash

header=$1
files=${2%/}

#{ echo -e "$header"
#  cat $file
#} > $tmp

function add_header_to_file(){
  tmp=$(mktemp)   # Create a temporary file
  trap "rm -f $tmp; exit 1" 0 1 2 3 13 15

  echo "Processing $file"

  cat $header $file > $tmp
  mv $tmp $file
  rm -f $tmp
  trap 0
}

if [ -d $files ]; then
  # a directory of files
  for file in $files/*; do
    if [ -f $file ]; then
       add_header_to_file
    fi
  done
elif [ -f $files ]; then
  # a regular file
  file=$files
  add_header_to_file
else
  echo "You need to provide the file with header and either a single file to apply it to or a directory of files"
  echo "e.g. ./addHeader header_file.txt file_needing_header.tsv OR ./addHeader header_file.txt folder_of_files/"
fi
