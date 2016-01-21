args <- commandArgs(trailingOnly = TRUE)
parentPid <- as.numeric(args[1])
waitTime <- as.numeric(args[2])
pidFile <- args[3]
write(Sys.getpid(), pidFile)
Sys.sleep(waitTime)
require(tools)
pskill(parentPid)
