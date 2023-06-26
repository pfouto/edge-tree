#!/bin/sh

log_file=$1
shift

echo " "

mkdir -p $(dirname "${log_file}")

echo "$(date) Running server jar..." > /proc/1/fd/1

echo "$@" > /proc/1/fd/1

java -DlogFilename=${log_file} -jar tree.jar "$@" 2> ${log_file}.err 1> ${log_file}.out

#java \
#-DlogFilename=${log_file} \
#-Djava.library.path=./lib/sigar-bin \
#-Dcassandra-foreground=yes \
#-XX:+CrashOnOutOfMemoryError \
#-Dcassandra.disable_tcactive_openssl=true \
#-jar tree.jar "$@" 2> ${log_file}.err 1> ${log_file}.out

#echo "Chmoding..." > /proc/1/fd/1
#chown ${uid}:${gid} /logs/${exp_name}/${c_name}.log

echo "$(date) Goodbye" > /proc/1/fd/1
