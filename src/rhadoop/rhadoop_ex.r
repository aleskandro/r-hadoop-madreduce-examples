#!/bin/env Rscript

Sys.setenv(HADOOP_HOME="/usr/local/hadoop")
Sys.setenv(HADOOP_CMD="/usr/local/hadoop/bin/hadoop")

library(rmr2)
library(rhdfs)

ints = to.dfs(1:100)
calc = mapreduce(input = ints,
                   map = function(k, v) cbind(v, 2*v))

from.dfs(calc)
