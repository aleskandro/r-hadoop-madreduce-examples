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

