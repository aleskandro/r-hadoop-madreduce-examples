#!/bin/env Rscript

Sys.setenv(HADOOP_HOME="/usr/local/hadoop")
Sys.setenv(HADOOP_CMD="/usr/local/hadoop/bin/hadoop")

library(rmr2)
library(rhdfs)
library(parallel)
library(ggplot2)

### Useful functions
toSecs <- function(ts) {
    ts <- as.numeric(ts)
    return (ts[1] * 3600 + ts[2] * 60 + ts[3])
}

# return a string without leading or trailing spaces
strip   <- function(str)  gsub("^\\s+|\\s+$", "", str)

# split line into a list
splitString <- function(line, str = " ") unlist(strsplit(line, str))

# return the IP in a string formatted as x.y.z.w.port
getIp <- function(str) paste(splitString(str, "\\.")[1:4], collapse = ".")

### MapReduce functions
mapPackets <- function(l) {
    line  <- strip(l)
    elems <- splitString(line, "[[:space:]]+")

    # Source IP
    src <- getIp(elems[3])

    # Destination IP
    dst <- getIp(elems[5])

    # Packet length
    pckLength <- strip(elems[8])
    return(paste(src, dst, pckLength))
}

mapTimestamps <- function(l) {
    # Timestamp
    line  <- strip(l)
    elems <- splitString(line, "[[:space:]]+")
    timestamp <- splitString(splitString(elems[1], "\\.")[1], ":")

    # Calculating the packets counter every 5 minutes
    if (is.na(actualTimestamp)) {
        actualTimestamp <<- timestamp
    }
    if (toSecs(timestamp) - toSecs(actualTimestamp) >= 300) {
        retPckCount <- packetsCount
        oldTimestamp<- actualTimestamp
        # Reset the counter and set the new start timestamp
        packetsCount    <<- 0
        actualTimestamp <<- timestamp
        return(paste(paste(oldTimestamp, collapse = ":"), retPckCount))
    } else {
        packetsCount <<- packetsCount + 1
        return(NA)
    }
}

reducePackets <- function(vv) {
    large  <- sum(as.integer(splitString(strip(vv))[3]) > 512)
    total  <- length(vv)
    asList <- sapply(vv, strsplit, " ")

    df     <- as.data.frame.matrix(do.call(rbind, asList))
    colnames(df)  <- c("client", "server", "length")

    topTenClients <- aggregate(x = as.numeric(as.character(df$length)), FUN=sum, by=list(unique.values = df$client))
    topTenServers <- aggregate(x = df$server, FUN=length, by=list(unique.values = df$server))

    myList <- list(total = total, large = large, topTenClients = topTenClients, topTenServers = topTenServers)

    return(keyval("reduced", myList))
}

reduceTimestamps <- function(vv) {
    asList <- sapply(vv, strsplit, " ")
    df     <- as.data.frame.matrix(do.call(rbind, asList))
    colnames(df)  <- c("datetime", "count")
    return(keyval("timestamps",list(timestamps=df)))
}

mapper <- function(k, l) {
    myList  <- splitString(l, "\n")
    packets <- mclapply(myList, mapPackets)
    packetsCount    <<- 0
    actualTimestamp <<- NA

    timestamps <- lapply(myList, mapTimestamps)
    timestamps <- timestamps[!is.na(timestamps)]
    timestamps <- keyval("timestamp", timestamps)
    packets    <- keyval("packet", packets)
    return(keyval(c(packets$key, timestamps$key), c(packets$val, timestamps$val)))
}

reducer <- function(k, vv) {
    if (k == "packet") {
        return(reducePackets(vv))
    }

    if (k == "timestamp") {
        return(reduceTimestamps(vv))
    }
}

# Printing/Plotting functions for the statistics at the end

topTen <- function(x) {
    index <- with(x, order(-x))
    return(head(x[index,], 10))
}

printStats <- function(t) {
    totalPackets <- t$total
    largePackets <- t$large
    pLargePackets<- largePackets / totalPackets * 100

    cat("Total packets: ", totalPackets, " - Large packets: ", pLargePackets, " - Small packets: ", 100 - pLargePackets, "\n")

    cat("\nTop ten clients:\n")
    out <- topTen(t$topTenClients)
    colnames(out) <- c("ClientIP", "Bytes sent")
    print(out)

    cat("\nTop ten servers:\n")
    out <- topTen(t$topTenServers)
    colnames(out) <- c("ServerIP", "Packets count")
    print(out)
}

savePlot <- function(timestamps) {
    # Putting time based output to a line chart and saving to pdf 
    # (no gui available through the docker container provided)
    thePlot <- ggplot(data=timestamps, aes(x=datetime, y=count, group=1)) +
        geom_line() + geom_point() +
        theme(axis.text.x = element_text(angle = 90, hjust = 1, size=5))

    ggsave(filename="plot.pdf", plot=thePlot, width=10, height=5)
}

hdfs.init()
job <- mapreduce(input= "/user/root/tcpdump.log",
                    input.format = 'text',
                    map    = mapper,
                    reduce = reducer)

output = from.dfs(job)
printStats(output$val)
savePlot(output$val$timestamps)

