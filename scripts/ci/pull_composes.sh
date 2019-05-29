#!/usr/bin/env bash

if [ -d compose_remote/ ]; then
    echo deleting existing compose_remote directory
    rm -rf compose_remote
fi
echo Pulling compose files from remotes
if [ ! -d compose_remote/ ]; then
    echo Creating compose_remote directory
    mkdir compose_remote
fi

cd compose_remote
mkdir eva-catalog && cd eva-catalog
git init && git remote add origin git@github.com:Workiva/eva-catalog.git
git fetch --tags
echo Checking out compose for eva-catalog at $(git describe --tags $(git rev-list --tags --max-count=1))
git checkout $(git describe --tags $(git rev-list --tags --max-count=1)) -- docker-compose.yml
cd ../..
cp compose_remote/eva-catalog/docker-compose.yml ./compose_remote/local-compose-eva-catalog.yml
rm -rf compose_remote/eva-catalog
echo local-compose-eva-catalog.yaml cloned to compose_remote dir
