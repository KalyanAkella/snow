#
# Socket Implementation
#

#**** allow user to be different on different machines
#**** allow machines to be selected from a hosts list
newSOCKnode <- function(machine = "localhost", numInstances, ...,
                        options = defaultClusterOptions, rank) {
    # **** allow some form of spec here
    # **** make sure options are quoted
    options <- addClusterOptions(options, list(...))
    if (is.list(machine)) {
        options <- addClusterOptions(options, machine)
        machine <- machine$host
    }
    outfile <- getClusterOption("outfile", options)
    if (machine == "localhost") master <- "localhost"
    else master <- getClusterOption("master", options)
    port <- getClusterOption("port", options)
    manual <- getClusterOption("manual", options)
    eventhandler <- getClusterOption("eventhandler", options)

    ## build the local command for starting the worker
    homogeneous <- getClusterOption("homogeneous", options)
    if (getClusterOption("useRscript", options)) {
        if (homogeneous) {
            rscript <- shQuoteIfNeeded(getClusterOption("rscript", options))
            snowlib <- getClusterOption("snowlib", options)
            script <- shQuoteIfNeeded(file.path(snowlib, "snow", "RSOCKnode.R"))
            env <- paste("MASTER=", master,
                         " PORT=", port,
                         " OUT=", outfile,
                         " SNOWLIB=", snowlib,
                         " EVENTHANDLER=", eventhandler, sep="")
            cmd <- paste(rscript, script, env)
        }
        else {
            script <- "RunSnowWorker RSOCKnode.R"
            env <- paste("MASTER=", master,
                         " PORT=", port,
                         " OUT=", outfile,
                         " EVENTHANDLER=", eventhandler, sep="")
            cmd <- paste(script, env)
        }
    }
    else {
        if (homogeneous) {
            scriptdir <- getClusterOption("scriptdir", options)
            script <- shQuoteIfNeeded(file.path(scriptdir, "RSOCKnode.sh"))
            rlibs <- paste(getClusterOption("rlibs", options), collapse = ":")
            rprog <- shQuoteIfNeeded(getClusterOption("rprog", options))
            env <- paste("MASTER=", master,
                         " PORT=", port,
                         " OUT=", outfile,
                         " RPROG=", rprog,
                         " R_LIBS=", rlibs, sep="")
        }
        else {
            script <- "RunSnowNode RSOCKnode.sh"
            env <- paste("MASTER=", master,
                         " PORT=", port,
                         " OUT=", outfile, sep="")
        }
        cmd <- paste("env", env, script)
    }

    ## need timeout here because of the way internals work
    timeout <- getClusterOption("timeout")
    old <- options(timeout = timeout);
    on.exit(options(old))

    if (manual) {
      cat("Manually start", numInstances, "worker process(es) on", machine, "with\n    ", cmd, "\n")
      flush.console()
    } else {
      if (machine != "localhost") {
          rshcmd <- getClusterOption("rshcmd", options)
          user <- getClusterOption("user", options)
          cmd <- paste(rshcmd, "-l", user, machine, cmd)
      }
    }

    pidFile <- startSocketBreaker(numInstances, rscript, snowlib)

    cl <- vector("list", numInstances)
    for(i in seq(numInstances)) {

      if (!manual) {
        launchCommandWithIndex(cmd, i)
      }

      con <- socketConnection(port = port, server=TRUE, blocking=TRUE, open="a+b")
      cl[[i]] <- structure(list(con = con, host = machine, rank = rank), class = "SOCKnode")
    }

    killSocketBreaker(pidFile)
    cl
}

startSocketBreaker <- function(numInstances, rscript, snowlib) {
  sockWaitTime <- 10 * numInstances
  pid <- Sys.getpid()
  pidFile <- tempfile(pattern = "pidFile.")
  launchCommand(paste(rscript, shQuoteIfNeeded(file.path(snowlib, "snow", "SOCKBreaker.R")), pid, sockWaitTime, pidFile))
  return(pidFile)
}

killSocketBreaker <- function(pidFile) {
  require(tools)
  pskill(scan(pidFile))
  unlink(pidFile)
}

launchCommand <- function(cmd){
  if (.Platform$OS.type == "windows") {
      system(cmd, wait = FALSE, input = "")
  } else {
      system(cmd, wait = FALSE)
  }
}

launchCommandWithIndex <- function(cmd, index){
  launchCommand(paste(cmd, index))
}

makeSOCKmaster <- function(master = Sys.getenv("MASTER"),
                           port = Sys.getenv("PORT")) {
    port <- as.integer(port)
    ## maybe use `try' and sleep/retry if first time fails?
    ## need timeout here because of the way internals work
    timeout <- getClusterOption("timeout")
    old <- options(timeout = timeout);
    on.exit(options(old))
    con <- socketConnection(master, port = port, blocking=TRUE, open="a+b")
    structure(list(con = con), class = "SOCKnode")
}

closeNode.SOCKnode <- function(node) close(node$con)

sendData.SOCKnode <- function(node, data) {
##     timeout <- getClusterOption("timeout")
##     old <- options(timeout = timeout);
##     on.exit(options(old))
    serialize(data, node$con)
}

recvData.SOCKnode <- function(node) {
##     timeout <- getClusterOption("timeout")
##     old <- options(timeout = timeout);
##     on.exit(options(old))
    unserialize(node$con)
}

recvOneData.SOCKcluster <- function(cl) {
    socklist <- lapply(cl, function(x) x$con)
    repeat {
        ready <- socketSelect(socklist)
        if (length(ready) > 0) break;
    }
    n <- which(ready)[1]  # may need rotation or some such for fairness
    list(node = n, value = unserialize(socklist[[n]]))
}

appendDefaultInstancesToMachines <- function(names, defaultNumOfInstances) {
  for (i in seq(along=names)) {
    if (length(grep("^.+:[0-9]+$", names[[i]])) == 0) {
      names[[i]] <- paste(names[[i]], "1", sep=':')
    }
  }
  names
}

constructListOfSnowInstances <- function(names) {
  snowInstances <- vector("list", length(names))
  machines <- vector("character", length(names))
  for (i in seq(along=names)) {
    splits <- strsplit(names[[i]], ":")[[1]]
    machines[i] <- splits[1]
    snowInstances[[i]] <- as.numeric(splits[2])
  }
  names(snowInstances) <- machines
  snowInstances
}

makeSOCKcluster <- function(names, ..., options = defaultClusterOptions) {
    if (is.numeric(names))
        names <- rep('localhost', names[1])

    names <- appendDefaultInstancesToMachines(names, 1)
    snowInstances <- constructListOfSnowInstances(names)
    machines <- names(snowInstances)

    options <- addClusterOptions(options, list(...))
    connections <- vector("list", 0)
    for (i in seq(along=machines)) {
      machine <- machines[[i]]
      newConnections <- newSOCKnode(machine, snowInstances[[machine]], options = options, rank = i)
      connections <- c(connections, newConnections)
    }
    class(connections) <- c("SOCKcluster", "cluster")
    connections
}
