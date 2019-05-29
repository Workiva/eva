#!/usr/bin/env bash

#####################################################################
## Portions of this script is copied from:
## https://github.com/fabric8io-images/java/blob/14e31db422828a08ae25ff94ce3966b447ea28a8/images/jboss/openjdk8/jdk/run-java.sh
## Based on this article:
## https://developers.redhat.com/blog/2017/03/14/java-inside-docker/
## Thanks to @rosshendrickson-wf
#####################################################################

# Detected container limits
# If found these are exposed as the following environment variables:
#
# - CONTAINER_MAX_MEMORY
# - CONTAINER_CORE_LIMIT
#
# This script is meant to be sourced.

ceiling() {
    awk -vnumber="$1" -vdiv="$2" '
    function ceiling(x){
      return x%1 ? int(x)+1 : x
    }
    BEGIN{
      print ceiling(number/div)
    }
  '
}

# Based on the cgroup limits, figure out the max number of core we should utilize
core_limit() {
    local cpu_period_file="/sys/fs/cgroup/cpu/cpu.cfs_period_us"
    local cpu_quota_file="/sys/fs/cgroup/cpu/cpu.cfs_quota_us"
    if [ -r "${cpu_period_file}" ]; then
	local cpu_period="$(cat ${cpu_period_file})"

	if [ -r "${cpu_quota_file}" ]; then
	    local cpu_quota="$(cat ${cpu_quota_file})"
	    # cfs_quota_us == -1 --> no restrictions
	    if [ "x$cpu_quota" != "x-1" ]; then
		ceiling "$cpu_quota" "$cpu_period"
	    fi
	fi
    fi
}

max_memory() {
    # High number which is the max limit unti which memory is supposed to be
    # unbounded. 512 TB for now.
    local max_mem_unbounded="562949953421312"
    local mem_file="/sys/fs/cgroup/memory/memory.limit_in_bytes"
    if [ -r "${mem_file}" ]; then
	local max_mem="$(cat ${mem_file})"
	if [ ${max_mem} -lt ${max_mem_unbounded} ]; then
	    echo "${max_mem}"
	fi
    fi
}

limit="$(core_limit)"
if [ x$limit != x ]; then
    export EVA_CONTAINER_CORE_LIMIT="${limit}"
fi
unset limit

limit="$(max_memory)"
if [ x$limit != x ]; then
    export CONTAINER_MAX_MEMORY="$limit"
fi
unset limit


#####################################################################
## END COPIED SCRIPT
#####################################################################

## http://stackoverflow.com/questions/26621647/convert-human-readable-to-bytes-in-bash
to_bytes() {
    echo $(echo "$1" | awk '/[0-9]$/{print $1;next};/[gG]$/{printf "%u\n", $1*(1024*1024*1024);next};/[mM]$/{printf "%u\n", $1*(1024*1024);next};/[kK]$/{printf "%u\n", $1*1024;next}')
}

## Local profiling of a 10-minute user benchmark shows compressed class space quickly
## approaching and then remaining at 31 M. (v0.3.99)
JAVA_COMPRESSED_CLASS_SPACE_SIZE="${JAVA_COMPRESSED_CLASS_SPACE_SIZE:-256M}"

## Local profiling of a 10-minute user benchmark shows metaspace allocation quickly
## approaching and then remaining at 148 M. (v0.3.99)
JAVA_MAX_METASPACE_SIZE="${JAVA_MAX_METASPACE_SIZE:-512M}"

## For now not constraining code cache size. Default supposedly is 48 MB, which according
## to Java docs "is so large that the application will never fill the entire codecache."
## According to YourKit, local profiling puts us at 57 MB used. So....just gonna leave it
## for now. Even if we avoid a crash, the wrong constraint could cause code to stop
## compiling (JIT, transacted fns, etc.)
##
## JAVA_RESERVED_CODE_CACHE_SIZE="${JAVA_RESERVED_CODE_CACHE_SIZE:-128M}"

## This really shouldn't be allowed to be as low as 1G. That would be bad (see math):
CONTAINER_MAX_MEMORY=${CONTAINER_MAX_MEMORY:-${JAVA_XMX:-$(to_bytes "2G")}}

mem_remaining=$(expr $CONTAINER_MAX_MEMORY \
		     - $(to_bytes $JAVA_COMPRESSED_CLASS_SPACE_SIZE) \
		     - $(to_bytes $JAVA_MAX_METASPACE_SIZE) \
		     - $(to_bytes "128M") \
	             - $(to_bytes "256M")) ## for good measure

JAVA_XMX="${JAVA_XMX:-$mem_remaining}"
JAVA_XMS="${JAVA_XMS:-$mem_remaining}"

default_java_opts="-XX:-OmitStackTraceInFastThrow \
-XX:+UseG1GC -XX:MaxGCPauseMillis=50 \
-Xms${JAVA_XMS} -Xmx${JAVA_XMX} \
-XX:MaxMetaspaceSize=${JAVA_MAX_METASPACE_SIZE} \
-XX:CompressedClassSpaceSize=${JAVA_COMPRESSED_CLASS_SPACE_SIZE} \
-XX:NewRatio=3"

JAVA_OPTS="${JAVA_OPTS:-$default_java_opts}"
