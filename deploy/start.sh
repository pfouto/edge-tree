#!/bin/sh

exp_name=$1
c_name=$2
region=$3
regional_dc=$4
uid=$5
gid=$6

echo "Running jar..." > /proc/1/fd/1

java \
-DlogFilename=/logs/${exp_name}/${c_name}.log \
-Djava.library.path=./lib/sigar-bin \
-Dcassandra-foreground=yes \
-XX:+CrashOnOutOfMemoryError \
-Dcassandra.disable_tcactive_openssl=true \
-jar tree-fat-1.0-SNAPSHOT.jar hostname=${c_name} region=${region} datacenter=${regional_dc} > /proc/1/fd/1 2>&1

echo "Chmoding..." > /proc/1/fd/1

chown ${uid}:${gid} /logs/${exp_name}/${c_name}.log

echo "Goodbye" > /proc/1/fd/1
