#!/usr/bin/env Rscript
# The  reducer
modules::import('utils', attach = TRUE)

# Global variables
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

if (is.null(modules::module_name()))
    main()

