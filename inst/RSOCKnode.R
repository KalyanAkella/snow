local({
    master <- "localhost"
    port <- ""
    snowlib <- Sys.getenv("R_SNOW_LIB")
    outfile <- Sys.getenv("R_SNOW_OUTFILE") ##**** defaults to ""; document
    eventhandler <- ""

    args <- commandArgs()
    index <- tail(args, n=1)
    pos <- match("--args", args)
    args <- args[-(1 : pos)]
    for (a in args) {
        pos <- regexpr("=", a)
        name <- substr(a, 1, pos - 1)
        value <- substr(a,pos + 1, nchar(a))
        switch(name,
               MASTER = master <- value,
               PORT = port <- value,
               SNOWLIB = snowlib <- value,
               OUT = outfile <- value,
               EVENTHANDLER = eventhandler <- value)
    }

    if (! (snowlib %in% .libPaths()))
        .libPaths(c(snowlib, .libPaths()))
    library(methods) ## because Rscript as of R 2.7.0 doesn't load methods
    library(snow)

    if (port == "") port <- getClusterOption("port")

    outfile <- paste0(outfile, ".", index)
    sinkWorkerOutput(outfile)
    cat("starting worker for", paste(master, port, sep = ":"), "\n")
    source(paste0(snowlib, "/snow/RSOCKEventHandler.R"))
    if (file.exists(eventhandler)) source(eventhandler)
    slaveLoop(makeSOCKmaster(master, port))
})
