#!/bin/bash
HIVEDB_HOME="$PWD/target"

java -jar "$HIVEDB_HOME/hivedb-jar-with-dependencies.jar" -host $1 -db $2 -user $3 -pw $4