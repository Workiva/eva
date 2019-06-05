#!/bin/bash

# Checks if either the `docker build ...` command exited with '0' or
# if the dockerfile ends with FROM scratch, no image will be generated which
# causes exit code '1', however this is a valid condition from our perspective.

COLOR_NC='\e[0m'
COLOR_GREEN='\e[0;32m'
COLOR_RED='\e[0;31m'

docker build -f workivabuild.Dockerfile . 2>&1 | tee workivabuild.Dockerfile.output

finalLine=$(awk '/./{line=$0} END{print line}' workivabuild.Dockerfile.output)

regex="Successfully built*"
if [[ "$finalLine" == $regex ]]; then
    printf "${COLOR_GREEN}Dockerfile built successfully${COLOR_NC}"
    rm workivabuild.Dockerfile.output
    exit 0
fi

if [[ "$finalLine" == "No image was generated. Is your Dockerfile empty?" ]]; then
    printf "${COLOR_GREEN}Dockerfile built successfully${COLOR_NC}"
    rm workivabuild.Dockerfile.output
    exit 0
fi

printf "${COLOR_RED}Dockerfile built successfully${COLOR_NC}"
rm workivabuild.Dockerfile.output
exit 1
