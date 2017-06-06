#!/bin/bash

OS=$(uname -s)
if [ "${OS}" = "Darwin" ]
then
  SCRIPT="$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)/`basename "${BASH_SOURCE[0]}"`"
else
  SCRIPT=`readlink -f ${BASH_SOURCE[0]}`
fi

SCRIPT_DIR=`dirname ${SCRIPT}`

file="${SCRIPT_DIR}/../config/pm-analytics.properties"

if [ -f "$file" ]
then
  echo "$file found."

  while IFS='=' read -r key value
  do
    key=$(echo $key | tr '.' '_')
    eval "${key}='${value}'"
  done < "$file"

  echo "host=${host}"
  echo "port=${port}"
  echo "namespace=${namespace}"
else
  echo "$file not found."
  exit 1 #terminate with error
fi

# deploy CDAP jar
curl -w"\n" ${host}:${port}/v3/namespaces/${namespace}/artifacts/pm-analytics --data-binary @${SCRIPT_DIR}/../target/pm-analytics-0.1.0.jar
sleep 2
echo "deployed artifact"

# Create APP
curl -w"\n" -X PUT -H "Content-Type: application/json" ${host}:${port}/v3/namespaces/${namespace}/apps/c26Analytics -d '{ "artifact":{ "name": "pm-analytics", "version": "0.1.0", "scope": "user" } }'
sleep 3
echo "created CDAP app"

# start flow, 3 services
curl -w"\n" -X POST "${host}:${port}/v3/namespaces/${namespace}/apps/c26Analytics/flows/c26Flow/start"
curl -w"\n" -X POST "${host}:${port}/v3/namespaces/${namespace}/apps/c26Analytics/services/c26TripService/start"
curl -w"\n" -X POST "${host}:${port}/v3/namespaces/${namespace}/apps/c26Analytics/services/c26FeatureService/start"
curl -w"\n" -X POST "${host}:${port}/v3/namespaces/${namespace}/apps/c26Analytics/services/c26ModelService/start"
echo "started flowlets and services"