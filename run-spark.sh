#!/bin/bash

# Check for the correct number of arguments

if [ "$#" -ne 4 ]; then
	echo "Usage $0: <File_with_main_class> <number_of_threads> <name_of_jar_file> <fileset_path>"
	exit 1
fi
NO_OF_THREADS="$2"
NAME_OF_JARFILE="$3"
MAIN="$1"
FILESET_PATH="$4"
spark-submit --class "$MAIN" --master local["$NO_OF_THREADS"] ./target/scala-2.11/"$NAME_OF_JARFILE"_2.11-0.1.jar "$FILESET_PATH"