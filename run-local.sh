#!/usr/bin/env bash
SCRIPT_DIR=$(dirname $(readlink -f $BASH_SOURCE[0]))


set -e
set -x

#mvn package

# Use -DSpark.master=local[K]	to run locally with K cores

#Submit the job
mvn compile exec:java \
    -Dexec.classpathScope="compile" \
    -Dexec.cleanupDaemonThreads="false" \
    -Dexec.mainClass="dk.kb.WarcCookieReader" \
    -Dspark.master=local \
    -Dexec.args="$SCRIPT_DIR/*.warc.gz"
