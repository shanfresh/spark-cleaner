rsync -vtr --progress -e "ssh -o ProxyCommand='ssh -l haxiaolin -W %h:%p relay.xiaomi.com'" ./target/galaxy-fds-spark-cleaner-1.0-SNAPSHOT.jar haxiaolin@hadoop-build01:/home/haxiaolin
