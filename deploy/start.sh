#!/bin/sh

log_file=$1
shift

echo "Running server jar..." > /proc/1/fd/1

java \
-DlogFilename=${log_file}.log \
-Djava.library.path=./lib/sigar-bin \
-Dcassandra-foreground=yes \
-XX:+CrashOnOutOfMemoryError \
-Dcassandra.disable_tcactive_openssl=true \
-jar tree.jar "$@" 2> ${log_file}.err 1> ${log_file}.out

#echo "Chmoding..." > /proc/1/fd/1
#chown ${uid}:${gid} /logs/${exp_name}/${c_name}.log

echo "Goodbye" > /proc/1/fd/1
