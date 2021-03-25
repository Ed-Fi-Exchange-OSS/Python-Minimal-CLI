#!/bin/bash
# bash approach to testing
# requires "jq"

CUSTID=staging_edfisb31
YEAR=2018
LIMIT=100
BASEFOLDER=testing/20181220-serial

touch profile.log
mkdir -p ./${BASEFOLDER}/${CUSTID}/${YEAR}

## Get all records from all endpoints and save data from each endpoint to <endpoint>.json file
for endpoint in $(python3 edfi.py ${CUSTID} ${YEAR} getendpoints | grep -v  "Profile logging" | jq . | sed "s/\[//g" | sed "s/\]//g" | grep -v "^$" | sed 's/\"//g' | sed "s/,//g" | sed "s/ //g"); do
    python3 edfi.py ${CUSTID} ${YEAR} get $endpoint --output=./${BASEFOLDER}/${CUSTID}/${YEAR}/$endpoint.json --page=-1 --limit=${LIMIT}
    # ^^ gets all records for the endpoint and writes them to endpoint.json
done