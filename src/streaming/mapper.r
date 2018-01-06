#!/usr/bin/env Rscript
# Script for hadoop mapper (tcpdump log analyzer)
modules::import('utils', attach = TRUE)

# Global variables
actualTimestamp <<- NULL
packetsCount    <<- 0

# Map function
map <- function(line) {
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
        cat("timestamp\t", paste(paste(actualTimestamp, collapse = ":"), packetsCount, "\n"))
        # Reset the counter and set the new start timestamp
        packetsCount    <<- 0
        actualTimestamp <<- timestamp
    } else {
        packetsCount <<- packetsCount + 1
    }

    cat("packet\t", paste(src, dst, pckLength, "\n"))
}

main <- function() {
    con <- file("stdin", open = "r")
    while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0)
        map(line)
}

if (is.null(modules::module_name()))
    main()

