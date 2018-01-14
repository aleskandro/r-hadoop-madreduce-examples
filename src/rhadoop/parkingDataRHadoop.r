#!/bin/env Rscript

Sys.setenv(HADOOP_HOME="/usr/local/hadoop")
Sys.setenv(HADOOP_CMD="/usr/local/hadoop/bin/hadoop")

library(rmr2)
library(rhdfs)

#id,nodeId,timestamp,status
#0,3600,2016-01-01 08:49:10,1

# Mapper
mapper <- function(k, lines) {
  # A che serve k?
  myList <- unlist(strsplit(x = lines, split = "\n"))
  packets <- lapply(myList, function(line) { 
    # fai robe
    elements <- splitString(line, ",")
    id <- elements[0]
    nodeId <- elements[1]
    timestamp <- elements[2]
    status <- elements[3]
    paste(id, nodeId, timestamp, status)
    # TODO Completare con i pacchetti presi ogni tot tempo
  })
  keyval("packet", packets)
}

# TODO Reducer

reducer <- function(k, vv) {
  # fai robe
  # keyval("reduced", myList)
}


hdfs.init()
# count <- mapreduce(input="/user/root/parkingData2016.csv", input.format = 'csv', map = mapper, reduce = reducer)

# somePrint(from.dfs(count)$val)
