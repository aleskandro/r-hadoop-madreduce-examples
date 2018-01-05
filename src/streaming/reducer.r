#!/usr/bin/env Rscript
# The  reducer

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

# Global variables
topTenClientsByServers <<- list()
topTenClients <<- list()
topTenServers <<- list()
timestamps    <<- list()
totalPackets  <<- 0
largePackets  <<- 0
pLargePackets <<- 0

# Reduce function
reduce <- function(line) {
    line <- splitString(strip(line), "\t")
    key  <- line[1]
    value<- line[2]
    datas<- splitString(strip(value), " ")

    if (key == "packet") {
        totalPackets <<- totalPackets + 1
        length <- as.integer(datas[3])
        if (length > 512) {
            largePackets <<- largePackets + 1
        }
        topTenClients <<- addValueHelper(datas[1], length, topTenClients)
        topTenServers <<- addValueHelper(datas[2], 1, topTenServers)
        if (is.null(topTenClientsByServers[[datas[2]]]))
            topTenClientsByServers[[datas[2]]] <<- list()
        topTenClientsByServers[[datas[2]]] <<- addValueHelper(datas[1], length, topTenClientsByServers[[datas[2]]])
    }

    if (key == "timestamp") {
        timestamps <<- addValueHelper(datas[1], as.integer(datas[2]), timestamps)
    }
}

# Print the statistics at the end
printStats <- function() {
    pLargePackets = largePackets / totalPackets * 100

    cat("Total packets: ", totalPackets, " - Large packets: ", pLargePackets, " - Small packets: ", 100 - pLargePackets, "\n")
    topTenSorted(topTenClients, "\ntopTenClients:\n")
    topTenSorted(topTenServers, "\ntopTenServers:\n")

    cat("\nTopTen Clients by Servers: \n")
    invisible(sapply(names(topTenClientsByServers), function(n) {
            topTenSorted(topTenClientsByServers[[n]], paste("\nServer: ", n))
        }))
    cat("\nPackets count every 5 minutes:\n")
    invisible(sapply(names(timestamps), function(n) {
            cat(n, timestamps[[n]], '\n', sep='\t')
        }))
}

main <- function() {
    con <- file("stdin", open = "r")
    while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) 
        reduce(line)
    close(con)
    printStats()
}
    main()

