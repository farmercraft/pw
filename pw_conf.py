log_server_ip = "localhost"

pw_autodetect_source = True
pw_autodetect_home_source = False
pw_autodetect_plot_dir = "plots" # plot folder on each mountpoints. E.g setting "plots" = /hdd1/plots /hdd2/plots /hdd3/plots etc...
pw_autodetect_min_dst_source_size = 6 # in TB. Any mountpoint which is larger than 6TB will be taken as DST source, if not, then SRC source

#path and mountpoint
#pw_autodetect_source = False
src_plots_dir = { "/sdb/plots":"/sdb", "/sdc/plots":"/sdc", "/sdd/plots":"/sdd", "/sdf/plots":"/sdf", "/sde/plots":"/sde", "/sdh/plots":"/sdh", "/sdi/plots":"/sdi", "/sdj/plots":"/sdj", "/sdk/plots":"/sdk", "/home/f91/plots":"/"}
dst_plots_dir = { "/sdg/plots":"/sdg"}
