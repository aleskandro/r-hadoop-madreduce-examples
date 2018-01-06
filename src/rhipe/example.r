#!/bin/env Rscript
Sys.setenv(HADOOP_HOME="/usr/local/hadoop/")
Sys.setenv(HADOOP_BIN="/usr/local/hadoop/bin")
Sys.setenv(HADOOP_CONF_DIR="/usr/local/hadoop/conf")


library(rJava)
library(Rhipe)
library(testthat)

rhinit()

rhmkdir("/user/root/bin")
hdfs.setwd("/user/root/bin")
bashRhipeArchive("R.Pkg")
rhoptions(zips = "/user/root/bin/R.Pkg.tar.gz")
rhoptions(runner = "sh ./R.Pkg/library/Rhipe/bin/RhipeMapReduce.sh")

test_package("Rhipe","simple")
rhput("/code/src/housing/housing.txt", "/user/root/housing.txt")
rhexists("/user/root/housing.txt")

map1 <- expression({
  lapply(seq_along(map.keys), function(r) {
    line = strsplit(map.values[[r]], ",")[[1]]
    outputkey <- line[1:3]
    outputvalue <- data.frame(
      date = as.numeric(line[4]),
      units =  as.numeric(line[5]),
      listing = as.numeric(line[6]),
      selling = as.numeric(line[7]),
      stringsAsFactors = FALSE
    )
  rhcollect(outputkey, outputvalue)
  })
})

reduce1 <- expression(
  pre = {
    reduceoutputvalue <- data.frame()
  },
  reduce = {
    reduceoutputvalue <- rbind(reduceoutputvalue, do.call(rbind, reduce.values))
  },
  post = {
    reduceoutputkey <- reduce.key[1]
    attr(reduceoutputvalue, "location") <- reduce.key[1:3]
    names(attr(reduceoutputvalue, "location")) <- c("FIPS","county","state")
    rhcollect(reduceoutputkey, reduceoutputvalue)
  }
)

mr1 <- rhwatch(
  map      = map1,
  reduce   = reduce1,
  input    = rhfmt("/user/root/housing.txt", type = "text"),
  output   = rhfmt("/user/root/byCounty", type = "sequence"),
  readback = FALSE
)

rhex(mr1)
