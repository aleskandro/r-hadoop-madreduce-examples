#!/bin/bash
hdfs dfs -rm -r -f log.log
hdfs dfs -rm -r -f tcpdump.log
hdfs dfs -copyFromLocal tcpdump.log
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.0.jar -file ./utils.r -file ./mapper.r -mapper ./mapper.r -file ./reducer.r -reducer ./reducer.r -input tcpdump.log -output log.log
hdfs dfs -cat log.log/part* | less
