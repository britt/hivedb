#!/bin/bash
if [ -z "$HIVEDB_HOME" ]; then
	HIVEDB_HOME="$PWD/target"
fi

if [ -z "$1" -o -z "$2" -o -z "$3" -o -z "$4" ]; then
	echo "usage: install.sh <host> <database> <user> <password>"
else
	java -jar "$HIVEDB_HOME/hivedb-jar-with-dependencies.jar" -host $1 -db $2 -user $3 -pw $4
fi