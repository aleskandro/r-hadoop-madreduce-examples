#!/usr/bin/env Rscript

#install.packages("ggplot2")
library(ggplot2)

dataset <- read.table(file = "input.txt", header = TRUE, sep = "\t", col.names = c("time", "count", "roba"))
dataset$roba <- NULL
dataset <- dataset[order(dataset$time),]

myPlot <- ggplot(data=dataset, aes(x=time, y=count, fill=time)) +
            geom_bar(position = 'identity', stat="identity") +
            scale_y_continuous(expand = c(0,0), limits = c(0,700)) +
            guides(fill=FALSE) +
            theme(axis.text.x = element_text(angle = 45, hjust = 1, size = 4)) + 
            ggtitle("Packet count sampled every 5 minutes") 
  
ggsave(filename = "myPlot.pdf", 
       plot=myPlot, 
       path="/code/plots/", 
       width=10, height = 5)

system("echo ciao")
