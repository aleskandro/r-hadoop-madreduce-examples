## R & Hadoop for data analysis with MapReduce

### Dependencies

* Hadoop
* devtools (R)
* klmr/modules (R)
* roxygen2 (R)
* linux-coreutils: (cat, less, sort... )

## TCPDump Logs

### Streaming

To run the code without hadoop:

```
    $ cd src/streaming
    $ cat myTcpDump.log | ./mapper.r | ./reducer.r

```


#### Hadoop (and Docker)

The code was tested in a Docker container derived from 'sequenceiq/hadoop-docker' hub.docker.com, using a single node hadoop server.

To build and run the container (take care of permissions and SELinux):
```
    $ docker build -t rhadoop .
    $ docker run -v /ABSPATH/TO/THIS/REPO/src/:/code -it rhadoop /etc/bootstrap.sh -bash
```

In the container, to run the analyzer:
```
    # cd /code/
    # hdfs dfs -copyFromLocal $myTcpDumpLogFile $myTcpDumpLogFile
    # hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.0.jar \
        -file ./utils.r
        -file ./mapper.r    -mapper ./mapper.r \
        -file ./reducer.r   -reducer ./reducer.r \
        -input $MyTCPDumpLogFile -output log.log ' 
    # hdfs dfs -cat log.log/part* | less
```

hdfs dfs -rm log.log will delete the log directory if you need to restart the map-reduce as above.

WARNING: Verify the path of the hadoop-streaming-\*.jar file 

`src/streaming/script.sh` provide a simple way to be ran inside the docker container avoiding the handwritten commands above

## Using RHadoop

```
    $ docker build -t rhadoop .
    $ docker run -v /ABSPATH/TO/THIS/REPO/src/:/code -it rhadoop /etc/bootstrap.sh -bash
    # cd src/rhadoop
    # hdfs dfs -copyFromLocal ../../samples/tcpdump.log 
    # ./rhadoop.r
```

## Using RHipe

Not working on Hadoop 2.7: https://github.com/delta-rho/RHIPE/issues/45
