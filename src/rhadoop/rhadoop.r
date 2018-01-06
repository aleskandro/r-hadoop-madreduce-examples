#!/bin/env Rscript

Sys.setenv(HADOOP_HOME="/usr/local/hadoop")
Sys.setenv(HADOOP_CMD="/usr/local/hadoop/bin/hadoop")

library(rmr2)
library(rhdfs)

### Useful functions

toSecs <- function(ts) {
    ts <- as.numeric(ts)
    return (ts[1] * 3600 + ts[2] * 60 + ts[3])
}

# return a string without leading or trailing spaces
strip   <- function(str)  gsub("^\\s+|\\s+$", "", str)

# split line into a list
splitString <- function(line, str) unlist(strsplit(line, str))

# return the IP in a string formatted as x.y.z.w.port
getIp <- function(str) paste(splitString(str, "\\.")[1:4], collapse = ".")

# an helper to populate the stats lists
addValueHelper <- function(key, value, lst) {
    if (is.null(lst[[key]])) {
        lst[[key]] = value
    } else {
        lst[[key]] = lst[[key]] + value
    }
    return(lst)
}

topTenSorted <- function(toSort, message) {
    i <- 0
    cat(message, '\n')
    ordered <- toSort[order(unlist(toSort), decreasing=TRUE)][1:10]
    invisible(sapply(names(ordered), function(n) {
        if (!is.na(n))
            cat(n, ordered[[n]], "\n", sep="\t")
    }))
}


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
    timestamp <- splitString(splitString(elems[1], "\\.")[1], ":")

    # Calculating the packets counter every 5 minutes
    if (is.null(actualTimestamp)) {
        actualTimestamp <<- timestamp
    }
    if (toSecs(timestamp) - toSecs(actualTimestamp) >= 300) {
        fun("timestamp\t", paste(paste(actualTimestamp, collapse = ":"), packetsCount, suffix))
        # Reset the counter and set the new start timestamp
        packetsCount    <<- 0
        actualTimestamp <<- timestamp
    } else {
        packetsCount <<- packetsCount + 1
    }
}

reducer <- function(k, vv) {
    total <<- 0
    large <<- 0
    topTenClientsByServers <<- list()
    topTenClients <<- list()
    topTenServers <<- list()
    lapply(vv, function(pck) {
        datas <- splitString(strip(pck), " ")
        total <<- total + 1
        length <- as.integer(datas[3])
        topTenClients <<- addValueHelper(datas[1], length, topTenClients)
        topTenServers <<- addValueHelper(datas[2], 1, topTenServers)
        if (is.null(topTenClientsByServers[[datas[2]]]))
            topTenClientsByServers[[datas[2]]] <<- list()
        topTenClientsByServers[[datas[2]]] <<- addValueHelper(datas[1], length, topTenClientsByServers[[datas[2]]])

        if (length > 512)
            large <<- large + 1
    })
    myList <- list(Total = total, large = large, topTenClients = topTenClients,
                   topTenServers = topTenServers, 
                   topTenClientsByServers = topTenClientsByServers)
    keyval("reduced", myList)

}

mapper <- function(k, l) {
    myList <- unlist(strsplit(x = l, split = "\n"))
    packets <- lapply(myList, function(line) { 
        mapPackets(line)
    })
    keyval("packet", packets)
}

hdfs.init()

count <- mapreduce(input="/user/root/tcpdump.log",
                    input.format = 'text',
                    map = mapper,
                    reduce = reducer)

# Print the statistics at the end
printStats <- function(t) {
    names(t)
    totalPackets <- t$Total
    largePackets <- t$large
    topTenClients <- t$topTenClients
    topTenServers <- t$topTenServers
    topTenClientsByServers <- t$topTenClientsByServers
    pLargePackets = largePackets / totalPackets * 100

    cat("Total packets: ", totalPackets, " - Large packets: ", pLargePackets, " - Small packets: ", 100 - pLargePackets, "\n")
    topTenSorted(topTenClients, "\ntopTenClients:\n")
    topTenSorted(topTenServers, "\ntopTenServers:\n")

    cat("\nTopTen Clients by Servers: \n")
    invisible(sapply(names(topTenClientsByServers), function(n) {
            topTenSorted(topTenClientsByServers[[n]], paste("\nServer: ", n))
        }))
}

printStats(from.dfs(count)$val)
