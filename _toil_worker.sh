#!/bin/bash
if [ -z $TOIL_INSTALL ]; then
    export TOIL_INSTALL=/opt/toil/
fi
source ${TOIL_INSTALL}/venv2.7/bin/activate
${TOIL_INSTALL}/venv2.7/bin/_toil_worker $@
