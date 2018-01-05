#!/usr/bin/env Rscript
# Script for hadoop mapper (tcpdump log analyzer)

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
actualTimestamp <<- NULL
packetsCount    <<- 0

# Mapper function (the fun parameter must be a 2 arguments function (es. cat or keyval from rmr2))
map <- function(line, fun, suffix = "") {
    line  <- strip(line)
    elems <- splitString(line, "[[:space:]]+")

    # Source IP
    src <- getIp(elems[3])

    # Destination IP
    dst <- getIp(elems[5])

    # Packet length
    pckLength <- elems[8]
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

    fun("packet\t", paste(src, dst, pckLength, suffix))
}

main <- function() {
    con <- file("stdin", open = "r")
    while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0)
        map(line, cat, "\n")
}

    main()

