#!/bin/bash

. ./pw_conf.sh

NEXT_PLOT_DIR="nothing"
CUR_PLOT_DIR="nothing"

. ./pw_common.sh

check_dst_plot_dir () {
	if [ ! -d "$1" ]; then
		echo "Cannot find destination dir: $1"
		return 1;
	fi

	mount | grep `realpath $1` > /dev/null 2>&1
	if [ ! $? -eq 0 ]; then
		echo "Destination dir $1 is not mounted ?!"
		return 1;
	fi

	return 0;
}

check_missing_package() {
	which $1

	if [ ! $? -eq 0 ]; then
		echo "Installing package $2"

		echo `hostname` | sudo -S apt-get -y install $2
		if [ ! $? -eq 0 ]; then
			echo "Fail to install $2"
			return 1;
		fi
		echo "Done $2"
	fi
	return 0;
}

check_avail_space() {
	CMD=`df -BG --output="avail" $1 | tail -1 |sed s#G##`
	if [ $CMD -lt 200 ]; then
		return 1;
	fi
	return 0;
}

update_plot_dir_stat() {
	for ((i=0;i<${#DST_PLOT_DIR[@]};i++)) do
		check_dst_plot_dir ${DST_PLOT_DIR[i]}
		DST_PLOT_NREADY[i]=$?
		if [ ${DST_PLOT_NREADY[i]} -eq 0 ]; then
			check_avail_space ${DST_PLOT_DIR[i]}
			DST_PLOT_FULL[i]=$?
			if [ ${DST_PLOT_FULL[i]} -eq 1 ]; then
				sreport_full ${DST_PLOT_DIR[i]}
			fi
		else
			sreport_nready ${DST_PLOT_DIR[i]}
		fi
		echo i: $i dir: ${DST_PLOT_DIR[i]} nready: ${DST_PLOT_NREADY[i]} full: ${DST_PLOT_FULL[i]}
	done
}

check_missing_package inotifywait inotify-tools
if [ ! $? -eq 0 ]; then
	exit 1;
fi

for ((i=0;i<${#PLOTS_DIR[@]};i++)) do
	PLOTS_DIR[i]=`realpath ${PLOTS_DIR[i]}`/plots
done

secho "Watching plot dirs: ${PLOTS_DIR[*]}"

for ((i=0;i<${#DST_PLOT_DIR[@]};i++)) do
	DST_PLOT_DIR[i]=`realpath ${DST_PLOT_DIR[i]}`
	secho "Target plot dir $i: ${DST_PLOT_DIR[i]}"
done

update_plot_dir_stat

inotifywait -mq --format '%w%f' --event close_write,moved_to ${PLOTS_DIR[*]} | while read file
do
	if [ ! "${file#*.}" = "plot" ]; then
		continue;
	fi

	update_plot_dir_stat

	NEXT_PLOT_DIR="nothing"

	for ((i=0;i<${#DST_PLOT_DIR[@]};i++)) do
		if [ ${DST_PLOT_NREADY[i]} -eq 1 ]; then
			continue;
		fi

		if [ ${DST_PLOT_FULL[i]} -eq 1 ]; then
			continue;
		fi

		NEXT_PLOT_DIR=${DST_PLOT_DIR[i]}

		if [ "$CUR_PLOT_DIR" = "$NEXT_PLOT_DIR" ]; then
			continue;
		else
			break;
		fi
	done

	echo next_dir: $NEXT_PLOT_DIR

	if [ "$NEXT_PLOT_DIR" = "nothing" ]; then
		secho "No available plot dir to write"
		sreport_full
		continue;
	fi

	SRC_PLOT=`dirname $file`
	SRC_PLOT=`dirname $SRC_PLOT`

	check_avail_space $SRC_PLOT
	if [ $? -eq 1 ]; then
		./pw_cp_hi.sh $file $NEXT_PLOT_DIR/plots &
	else
		./pw_cp_low.sh $file $NEXT_PLOT_DIR/plots &
	fi

	CUR_PLOT_DIR=$NEXT_PLOT_DIR
done
