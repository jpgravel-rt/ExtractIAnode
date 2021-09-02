library(dplyr)
library(lubridate)





left <- function(str, len) {
  return(substr(str, 1, len))
}


right <- function(str, len) {
  return(substr(str, nchar(str) - len + 1, nchar(str)))
}


get_tags <- function(plant) {
  file_data <- readLines(paste0("data/points-", plant,".txt")) %>% data.frame(path = .)
  if (plant == "AAR") {
    plant_tags <- file_data %>%
      transmute(path,
                tag = right(path, nchar(path) - 13),
                plant = substr(tag, 1, 3),
                pot = ifelse(right(tag, 7) == "IMFC.PV",
                             substr(tag, 15, 18),
                             ifelse(substr(tag, 11, 11) == "A", 
                                    paste0("1", substr(tag, 12, 14)), 
                                    paste0("2", substr(tag, 12, 14)))),
                kind = ifelse(right(tag, 8) == "VCUVE.PV", "v",
                              ifelse(right(tag, 7) == "IMFC.PV", "i",
                                     paste0("iano", substr(tag,22, 23)))))  
    return(plant_tags)
  }
  else if (plant == "ALM") {
    plant_tags <- file_data %>%
      transmute(path,
                tag = right(path, nchar(path) - 13),
                plant = substr(tag, 1, 3),
                pot = ifelse(right(tag, 7) == "IMFC.PV",
                             substr(tag, 16, 19),
                             substr(tag, 12, 15)),
                kind = ifelse(right(tag, 8) == "VCUVE.PV", "v",
                              ifelse(right(tag, 7) == "IMFC.PV", "i",
                                     paste0("iano", substr(tag,23, 24)))))  
    return(plant_tags)
  }
}


get_max_ts_per_pot <- function(sc, plant, tags) {
  plant_ianode_1s <- tbl(sc, "ianode_1s")
  print("Get the most recent timestamp for each pot.")
  plant_ianode_1s %>%
    filter(plant == plant) %>%
    select(pot, year, month) %>%
    group_by(pot, year) %>%
    summarise(month = max(month, na.rm = T)) %>%
    slice_max(year) %>%
    inner_join(plant_ianode_1s, by = c("pot", "year", "month")) %>%
    select(pot, ts) %>%
    group_by(pot) %>%
    summarize(max_ts = max(ts, na.rm = T)) %>%
    mutate(pot = as.character(as.integer(pot))) %>%
    arrange(pot) %>%
    collect()
}


get_creation_dates <- function(tags, client) {
  pexlib$extractor$update_creation_date
}


create_intervals <- function(plant_max_ts, plant_tags, nday) {
  plant_max_ts %>%
    inner_join(plant_tags, by = "pot") %>%
    mutate(start_time = max_ts + dseconds(1),
           end_time = start_time + ddays(nday),
           sync_time = start_time) %>%
    select(-max_ts) %>%
    arrange(pot, tag)
}


extract_ianode <- function(plant, extraction_intervals) {
  
  data_buffer <- paste0("hdfs://casagzclem1/tmp/extract_ianode_", plant, "_", 
                        stringi::stri_rand_strings(1, 8))
  pot_count <<- 0
  extraction_intervals %>%
    group_by(pot) %>%
    group_walk(extract_pot_ianode, data_buffer, plant)
  
  
  print("Moving the daily data from the temporary parquet to the final Hive table.")
  spark_read_parquet(sc, 
                     name = "temp_ianode", 
                     path = data_buffer, 
                     overwrite = T) %>%
    spark_write_table("ianode_1s", mode = "append")
  
  data_buffer_path <- right(data_buffer, 32)
  system(paste0("hdfs dfs -rm -R ", data_buffer_path))
}


extract_pot_ianode <- function(.x, .y, data_buffer, plant) {
  
  i <<- 0
  pot <- .y$pot
  pot_tags <- as.data.frame(.x)
  tag_count <- nrow(pot_tags)
  rownames(pot_tags) <- pot_tags$path
  
  print(paste0("Extraction for pot ", pot, "."))
  
  pot_data <- lapply(pot_tags$path, function(tag_path) {
    row <- pot_tags[tag_path,]
    i <<- i + 1
    print(paste0("[", i, "/", tag_count, "] ", tag_path))
    pexlib$extractor$extract_dataframe(
        client,
        tag_path,
        start_time = as.character(row$start_time),
        end_time   = as.character(row$end_time),
        sync_time  = as.character(row$sync_time),
        interval   = "1s",
        stream     = "Recorded") %>%
      mutate(ts = as.POSIXct(ts))
  }) %>%
    bind_rows() %>%
    select(-strval)
  
  # add dummy data to make sure all columns are created during pivot.
  placeholder_ts <- (pot_tags %>% head(1))$start_time - dseconds(1)
  padding_row <- pot_tags %>%
    transmute(tag, ts = placeholder_ts, numval = NA)
  pot_data <- pot_data %>%
    bind_rows(padding_row) %>%
    arrange(tag, ts)
  
  # make the pivot
  print(paste0("Pivoting pot ", pot, " data."))
  pivoted_pot_data <- pot_data %>% 
    inner_join(plant_tags, by = "tag") %>% 
    transmute(pot, 
              year = as.integer(year(ts)), 
              month = as.integer(month(ts)), 
              ts = floor_date(ts, "seconds"), 
              kind, 
              val = numval) %>% 
    pivot_wider(id_cols = c("pot", "year", "month", "ts"), 
                names_from = kind,
                values_from =  val,
                values_fn = first) %>%
    filter(ts > placeholder_ts)
  
  
  if (plant == "AAR") {
    pivoted_pot_data <- pivoted_pot_data %>%
      transmute(ts,
                iano01, iano02, iano03, iano04, iano05, iano06, iano07, iano08, iano09, iano10,
                iano11, iano12, iano13, iano14, iano15, iano16, iano17, iano18, iano19, iano20, 
                iano21, iano22, iano23, iano24, i, v, plant="AAR", pot, year, month)
  }
  else if (plant == "ALM") {
    pivoted_pot_data <- pivoted_pot_data %>%
      transmute(ts,
                iano01, iano02, iano03, iano04, iano05, iano06, iano07, iano08, iano09, iano10,
                iano11, iano12, iano13, iano14, iano15, iano16, iano17, iano18, iano19, iano20,
                iano21=NA, iano22=NA, iano23=NA, iano24=NA, i, v, plant="ALM", pot, year, month)
  }
  
  
  print(paste0("Saving ", nrow(pivoted_pot_data), " rows."))
  sdf_pivoted_pot_data <- copy_to(sc, 
                                  df = pivoted_pot_data, 
                                  name = "pivoted_pot_data", 
                                  overwrite = T)
  
  sdf_pivoted_pot_data %>%
    spark_write_parquet(data_buffer, mode = "append")
  
  
  pot_count <<- pot_count + 1
  if (pot_count %% 10 == 0) {
    print("Refresh spark session.")
    spark_disconnect(sc)
    sc <<- ConnectToSpark("reduction")
  }
  
}
