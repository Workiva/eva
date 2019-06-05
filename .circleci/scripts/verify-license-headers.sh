#!/bin/bash

# Verifies if the license headers are present in source-code files identified by extension
# Grabs the first line of every discovered file and checks it against the provided regex

# Parameter Listing:
# extension to search for
# regex
# file-names to exclude <var-args>

COLOR_NC='\e[0m'
COLOR_GREEN='\e[0;32m'
COLOR_CYAN='\e[0;36m'
COLOR_RED='\e[0;31m'
COLOR_YELLOW='\e[1;33m'

EXTENSION=$1
REGEX=$2
EXCLUDE_LIST=( "$@" )
# Remove First Two Arguments
EXCLUDE_LIST=( ${EXCLUDE_LIST[@]:2} )

printf $COLOR_CYAN
printf "Extension - $EXTENSION\n"
printf "REGEX - $REGEX\n"
printf "EXCLUDE LIST - ${EXCLUDE_LIST[*]}\n\n"
printf "$COLOR_NC\n"

missingHeader=false
for file in $(find . -name "*.$EXTENSION"); do
    # Check to see if we should exclude this file
    skipFile=false
    for excludeFile in "${EXCLUDE_LIST[@]}"; do
        if [[ $file == *"$excludeFile"* ]]; then
            printf "${COLOR_YELLOW}Skipping - ${file}${COLOR_NC}\n"
            skipFile=true
        fi
    done
    # Check the file headers
    if [[ "$skipFile" == false ]]; then
        headLine=$(head -n 1 $file)
        if ! [[ "$headLine" =~ $REGEX ]]; then
            printf "${COLOR_RED}\"${file}\" does not contain the proper licensing header!${COLOR_NC}\n"
            missingHeader=true
        fi
    fi
done

if [[ $missingHeader == false ]]; then
    printf "\n${COLOR_GREEN}All non-excluded *.${EXTENSION} files contained the licensing header.${COLOR_NC}\n"
    exit 0
else
    exit 1
fi
