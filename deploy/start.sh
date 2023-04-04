#!/bin/sh

log_file=$1
shift
#c_name=$1
#shift
#region=$1
#shift
#regional_dc=$1
#shift
#uid=$1
#shift
#gid=$1
#shift



echo "Running jar..." > /proc/1/fd/1

java \
-DlogFilename=${log_file} \
-Djava.library.path=./lib/sigar-bin \
-Dcassandra-foreground=yes \
-XX:+CrashOnOutOfMemoryError \
-Dcassandra.disable_tcactive_openssl=true \
-jar tree-fat-1.0-SNAPSHOT.jar "$@" > /proc/1/fd/1 2>&1

#echo "Chmoding..." > /proc/1/fd/1
#chown ${uid}:${gid} /logs/${exp_name}/${c_name}.log

echo "Goodbye" > /proc/1/fd/1
