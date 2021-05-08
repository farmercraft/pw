#!/bin/sh

. ./pw_common.sh

secho "[LOW] Copying $1 to $2"

ionice -c 3 cp $1 $2 && rm $1

secho "[LOW] Done $1"
