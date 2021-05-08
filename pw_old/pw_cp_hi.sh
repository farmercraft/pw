#!/bin/sh

. ./pw_common.sh

secho "[HIGH] Copying $1 to $2"

ionice -c 2 -n 0 cp $1 $2 && rm $1

secho "[HIGH] Done $1"
