#!/bin/bash

set -e
[ -n "$DEBUG" ] && set -x

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
    base_dir="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$base_dir/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
base_dir="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

lein clean
if [[ -z ${SKIP_TESTS+x} || ${SKIP_TESTS} == "false" ]]; then
    lein with-profile +aot,-dynamodb-local test
fi
lein jar
mv ./target/eva-*.jar ./
lein clean

lein with-profile +deployment uberjar
mv ./target/transactor.jar ./
lein clean

lein docs
cd docs/api && tar cvfz "${base_dir}/eva-api-docs.tgz" ./
mv "${base_dir}/eva-api-docs.tgz" ../../
cd "${base_dir}"
