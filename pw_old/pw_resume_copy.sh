#!/bin/bash

. ./pw_conf.sh
. ./pw_common.sh

for ((i=0;i<${#PLOTS_DIR[@]};i++)) do
	PLOTS_DIR[i]=`realpath ${PLOTS_DIR[i]}`/plots

	(cd ${PLOTS_DIR[i]} && find -name "*.plot" | xargs touch)
done
