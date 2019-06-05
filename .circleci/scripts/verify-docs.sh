#!/bin/bash
# Verifies that docs have been generated and updated
GREEN='\e[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

md5Command=md5sum
if [ $(uname -s) == "Darwin" ]; then
    md5Command=(md5 -q)
fi

# Store MD5 of Committed Documentation
MD5_ROOT_README_BEFORE=$(find README.md -type f -exec $md5Command {} \; | sort -k 2 | $md5Command)
MD5_DOCS_BEFORE=$(find ./docs/api -type f -exec $md5Command {} \; | sort -k 2 | $md5Command)

# Update Table of Contents
./.circleci/scripts/update-tocs.sh
# Update Codox documentation
lein docs

# Calculate Later
MD5_ROOT_README_AFTER=$(find README.md -type f -exec $md5Command {} \; | sort -k 2 | $md5Command)
MD5_DOCS_AFTER=$(find ./docs/api -type f -exec $md5Command {} \; | sort -k 2 | $md5Command)

# Verify root README.md
if [ "$MD5_ROOT_README_BEFORE" != "$MD5_ROOT_README_AFTER" ]; then
    printf "${RED}Aborting, parent README file TOC was not updated${NC}\n"
    printf "${RED}Run make update-tocs${NC}\n"
    exit 1
fi

# Verify ./documentation content
if [ "$MD5_DOCS_BEFORE" != "$MD5_DOCS_AFTER" ]; then
    printf "${RED}Aborting, ./docs/api was not updated${NC}\n"
    printf "${RED}Run lein docs${NC}\n"
    exit 1
fi

printf "${GREEN}Documentation verified successfully${NC}\n"
