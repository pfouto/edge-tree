#!/bin/sh

log_file=$1
shift

echo " "

mkdir -p $(dirname "${log_file}")

echo "$(date) Running server jar..." > /proc/1/fd/1

echo "$@" > /proc/1/fd/1

java -Xmx3g -DlogFilename=${log_file} -jar tree.jar "$@" 2> ${log_file}.err 1> ${log_file}.out

echo "$(date) Goodbye" > /proc/1/fd/1
