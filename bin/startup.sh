#!/bin/bash

OS=$(uname -s)
if [ "${OS}" = "Darwin" ]
then
  SCRIPT="$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)/`basename "${BASH_SOURCE[0]}"`"
else
  SCRIPT=`readlink -f ${BASH_SOURCE[0]}`
fi

SCRIPT_DIR=`dirname ${SCRIPT}`

if [[ ! -e ${SCRIPT_DIR}/../logs ]]; then
    mkdir ${SCRIPT_DIR}/../logs
fi

java -DLOG_FILE_DIR=${SCRIPT_DIR}/../logs -DLOG_FILE_NAME=vgf -cp ${SCRIPT_DIR}/../config:${SCRIPT_DIR}/../varspecs:${SCRIPT_DIR}/../lib-varcore/* com.threatmetrix.vgf.util.VgfUtil $@ 
