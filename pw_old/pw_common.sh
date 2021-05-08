#!/bin/sh

scmd() {
	logger -n 192.168.0.132 -p local0.alert $*
}

secho() {
	echo $*
	scmd $*
}

sreport_full() {
	if [ "$1" = "" ]; then
		secho "[FULL] All the destination plot disks are full"
	else
		secho "[FULL] Destination plot disk $1 is full"
	fi
}

sreport_nready() {
	if [ "$1" = "" ]; then
		secho "[NREADY] All the destination plot disks are not ready"
	else
		secho "[NREADY] Destination plot disk $1 is not ready"
	fi
}
