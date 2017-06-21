#!/usr/bin/env Rscript

# ###########################################
# ### COMPUTE AVERAGE OF SAMPLES AND ########
# ### CONFIDENCE INTERVAL AT 90% ############
# ###########################################
# Usage: script.R <sample1> <sample2> ...
# output: 
# > avg.txt # error columns append in the end
# ###########################################
# samples format:
#     "PAR1\tPAR2...\nPAR_N\t"
# ###########################################

arguments <- commandArgs(TRUE)
argc <- length(arguments)
files <- list()

for (i in 1:argc) {
	files[[i]] <- arguments[i]
}

csvs <- lapply(files, read.csv, sep='\t')
means <- Reduce( "+", csvs ) / length( csvs )
st.dev <- lapply( csvs, function(x) ( x - means )^2 )
st.dev <- sqrt( Reduce( "+", st.dev ) / length( st.dev ) )
error <- ( qnorm(0.950)*st.dev / sqrt(length( csvs ) )) # 90% conf. interval

avg_file <- "avg.txt"
avg <- cbind(means, error)

for (i in 1:length(avg)) {
	colnames(avg)[i] = paste("(", i, ")", colnames(avg)[i], sep="")
}

write.table(avg, avg_file, sep="\t", row.names=FALSE, col.names=TRUE)
