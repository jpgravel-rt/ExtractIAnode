library(synchronicity)



attachMutex <- function(name, timeout = 5) {
  exists <- file.exists(paste0("/dev/shm/", name))
  mutex <- NULL
  if(exists) {
    mutex <- boost.mutex(sharedName = name, timeout = timeout, create = FALSE)
  } else {
    mutex <- boost.mutex(sharedName = name, timeout = timeout, create = TRUE)
    system(paste0("chmod 777 /dev/shm/", name))
  }
  return(mutex)
}

#' Release a mutex by name or by mutex instance.
#' 
#' @param mutex can be a mutex instance (attachMutex) or a mutex name (character string)
releaseMutex <- function(mutex) {
  showErr <- getOption("show.error.messages")
  options(show.error.messages = FALSE)
  tryCatch({
    m <- mutex # in case it is an actual mutex instance.
    if (typeof(mutex) == "character") {
      # 'mutex' contains a name, create a mutex instance using the name.
      # If the name does not exist, the exception bubbles up.
      m <- boost.mutex(sharedName = mutex, timeout = 5, create = FALSE)
    }
    unlock(m) # m is guaranteed to be a mutex instance here.
  },
  finally = {
    # Restore options anyways.
    options(show.error.messages = showErr)
  })
}

unLockMutexByName <- function(name) {
  #options(show.error.messages = FALSE)
  mutex <-
    tryCatch({
      options(show.error.messages = FALSE)
      #Attach to mutex if it exists, error is sent...
      boost.mutex(sharedName = name, timeout = 5, create = FALSE)
    },
    error = function(e) {
      #Mutex does not exist... create it.
      options(show.error.messages = TRUE)
      boost.mutex(sharedName = name, timeout = 5, create = TRUE)
    })
  options(show.error.messages = TRUE)
  unlock(mutex)
  
  return(mutex)
}


getLastValue <- function(tagname, table) {
  impala <- dbConnect(odbc::odbc(), driver = "impala", host = "casagzclem1", 
                      port = 21050, AuthMech = 6, UID = "impala", PWD = "pwd", 
                      UseNativeQuery = 1, CurrentSchemaRestrictedMetadata = 1, 
                      schema = "vaudreuil")
  
  res <- dbSendStatement(impala, paste0("INVALIDATE METADATA ", table))
  if(dbHasCompleted(res)) dbClearResult(res)
  
  lastValue <- (tbl(impala, table) %>% group_by(tag) %>% 
                  filter(tag == tagname) %>% 
                  summarize(maxTs = max(ts, na.rm = T)) %>% select(tag, maxTs) %>% collect())$maxts[1]
  
  dbDisconnect(impala)
  
  if(is.na(lastValue)) lastValue <- as.POSIXct("2020-09-21", tz = "UTC")

  return(lastValue)
}

# Function to send a dataframe to HDFS, as a parquet file
sendDfToHive <- function(df, hiveSchema, hiveTableName, hivePartition) {
  dask <- import("dask")
  dd <- import("dask.dataframe")
  
  df_dask <- dd$from_pandas(df, npartitions=1)
  df_dask$to_parquet(path = paste0("hdfs:///user/hive/warehouse/", hiveSchema, ".db/", hiveTableName), 
                     write_index = F, 
                     append = T, 
                     engine = "pyarrow", 
                     compression = "none", 
                     partition_on = hivePartition
  )
  system(paste0("hive -e 'MSCK REPAIR TABLE ", hiveSchema, ".", hiveTableName, "'"))
}

# Function to send a dataframe to HDFS, as a parquet file
sendDfToHDFS <- function(df, location) {
  #library(reticulate)
  
  #py_run_string("import os")
  #py_run_string("os.environ['CLASSPATH'] = ''")
  #py_run_string("os.environ['HADOOP_HOME'] = '/etc/hadoop/'")
  #py_run_string("os.environ['SPARK_HOME'] = ''")
  #sudo 
  dask <- import("dask")
  dd <- import("dask.dataframe")
  #df=mtcars
  #location="hdfs:///rawdata/test2"
  df_dask <- dd$from_pandas(df, npartitions=1)
  #df_dask$to_csv(location)
  df_dask$to_parquet(path = location,
                     write_index = F,
                    engine = "pyarrow",
                     compression = "none")
}

pi_to_df <- function(date_debut, date_fin, tags, batchSize, interval)
{
  credentials <- config::get()
  df <- NULL
  while((date_debut + batchSize - 1) < date_fin) {
    print(paste0("############################# ", Sys.time(), " ###########################################"))
    print(paste0("Extracting data from ", date_debut, " to ", date_debut + batchSize - 1))
    print("#############################################################################################")
    
    for (i in 1:nrow(tags)) {
      point_path <- paste0(tags$server, tags$tag[[i]])
      creation_date <- tags$creationdate[[i]]
      interval <- tags$interval[[i]]
      if(creation_date < date_debut) {
        print(paste0("Extracting point ", point_path))
        df <- bind_rows(df, 
                        pex$extractDataframe(piWebServer = "https://casagszwebpi1/piwebapi/", 
                                             pointPath = point_path, 
                                             startTime = as.character(date_debut), 
                                             endTime = as.character(date_debut + batchSize), 
                                             syncTime = as.character(date_debut), 
                                             interval = interval, 
                                             username = credentials$username, 
                                             password = credentials$password, 
                                             dropDuplicates = F)
        )
      }
    }
    print(paste0("Done at ", Sys.time()))
    lastSuccess <- date_debut + batchSize - 1
    #saveRDS(lastSuccess, "/home/shared/shared-rsconnect-resources/vaudreuil_filtre/lastSuccess_filtre2.RDS")
    date_debut <- date_debut + batchSize
  }
  rm(credentials)
  return(df)
}


pi_to_hive_table <- function(date_debut, date_fin, tags, batchSize, interval, table)
{
  credentials <- config::get()
  
  while((date_debut + batchSize - 1) < date_fin) {
    print(paste0("############################# ", Sys.time(), " ###########################################"))
    print(paste0("Extracting data from ", date_debut, " to ", date_debut + batchSize - 1))
    print("#############################################################################################")
    df <- NULL
    for (i in 1:nrow(tags)) {
      point_path <- tags$tag[[i]]
      creation_date <- tags$creationdate[[i]]
      if(creation_date < date_debut) {
        print(paste0("Extracting point ", point_path))
        df <- bind_rows(df, 
                        pex$extractDataframe(piWebServer = "https://casagszwebpi1/piwebapi/", 
                                             pointPath = point_path, 
                                             startTime = as.character(date_debut), 
                                             endTime = as.character(date_debut + batchSize), 
                                             syncTime = as.character(date_debut), 
                                             interval = interval, 
                                             username = credentials$username, 
                                             password = credentials$password, 
                                             dropDuplicates = F)
        )
      }
    }
    if (!is.null(df)) {
      df <- df %>% mutate(year = lubridate::year(ts), month = lubridate::month(ts), day = lubridate::day(ts))
      print(paste0("Sending to HIVE at ", Sys.time()))
      sendDfToHive(df, "vaudreuil", table, list("year", "month", "day"))
    }
    print(paste0("Done at ", Sys.time()))
    lastSuccess <- date_debut + batchSize - 1
    #saveRDS(lastSuccess, "/home/shared/shared-rsconnect-resources/vaudreuil_filtre/lastSuccess_filtre2.RDS")
    date_debut <- date_debut + batchSize
  }
  rm(credentials)
}


pi_to_hdfs <- function(date_debut, date_fin, tags, batchSize, interval, location)
{
  credentials <- config::get()
  
  while((date_debut + batchSize - 1) < date_fin) {
    print(paste0("############################# ", Sys.time(), " ###########################################"))
    print(paste0("Extracting data from ", date_debut, " to ", date_debut + batchSize - 1))
    print("#############################################################################################")
    df <- NULL
    for (i in 1:nrow(tags)) {
      point_path <- paste0(tags$server, tags$tag[[i]])
      #creation_date <- tags$creationdate[[i]]
      #if(creation_date < date_debut) {
        print(paste0("Extracting point ", point_path))
        df <- bind_rows(df, 
                        pex$extractDataframe(piWebServer = "https://casagszwebpi1/piwebapi/", 
                                             pointPath = point_path, 
                                             startTime = as.character(date_debut), 
                                             endTime = as.character(date_debut + batchSize), 
                                             syncTime = as.character(date_debut), 
                                             interval = interval, 
                                             username = credentials$username, 
                                             password = credentials$password, 
                                             dropDuplicates = F)
        )
      #}
    }
    if (!is.null(df)) {
      print(paste0("Sending to HDFS at ", Sys.time()))
      sendDfToHDFS(df, location)
    }
    print(paste0("Done at ", Sys.time()))
    lastSuccess <- date_debut + batchSize - 1
    date_debut <- date_debut + batchSize
  }
  rm(credentials)
}


#---------------------------------
# Spark
#---------------------------------
ConnectToSpark = function(database = "default"){
  #Set home
  Sys.setenv(SPARK_HOME = "/usr/lib/spark/")
  
  #Initialize configuration
  config = spark_config()
  
  #Config
  config$`spark.yarn.am.memory`               = "1g"     #Default 512m. Amount of memory to use for the YARN Application Master in client mode. spark.yarn.am.memory + some overhead should be less than yarn.nodemanager.resource.memory-mb. In cluster mode, use spark.driver.memory instead.
  config$`spark.yarn.am.memoryOverhead`       = "1500m"     #Default "AM" memory * 0.10, with minimum of 384. Same as spark.driver.memoryOverhead, but for the YARN Application Master in client mode.
  config$`spark.yarn.am.cores`                = 1        #Default 1. Number of cores to use for the YARN Application Master in client mode. In cluster mode, use spark.driver.cores instead.
  config$`sparklyr.shell.executor-memory`     = "4g"     #Default 1g. Amount of memory to use per executor process.
  config$`sparklyr.shell.executor-cores`      = "1"      #Default 1 in Yarn, all the available cores on the worker in standalone. The number of cores to use on each executor.
  config$`sparklyr.shell.num-executors`       = "1"      #Default 1 in Yarn, all the available cores on the worker in standalone. The number of cores to use on each executor.
  config$`spark.kryoserializer.buffer.max.mb` = "512"
  config$`spark.sql.execution.arrow.enabled`  = TRUE
  config$`hive.exec.max.dynamic.partitions`   = TRUE
  

  #Connect
  sc = spark_connect(master = "yarn", app_name = paste0("sparklyr-", Sys.getenv("USER")), config = config)
  if (database != "default") {
    sdf_sql(sc, paste("USE", database))
  }
  
  sdf_sql(sc,"SET hive.exec.dynamic.partition.mode = nonstrict")
  sdf_sql(sc,"SET hive.merge.mapfiles = true")
  sdf_sql(sc,"SET hive.merge.mapredfiles = true")
  sdf_sql(sc,"SET hive.merge.size.per.task = 256000000")
  sdf_sql(sc,"SET hive.merge.smallfiles.avgsize = 134217728")
  sdf_sql(sc,"SET hive.exec.compress.output = true")
  sdf_sql(sc,"SET parquet.compression = snappy")
  
  return(sc)
}
