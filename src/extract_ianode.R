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



get_min_ts_per_pot <- function(sc, plant_code, table_name = "ianode_1s") {
  plant_ianode_1s <- tbl(sc, "ianode_1s") %>%
    filter(plant == plant_code)
  print("Get the most ancient timestamp for each pot.")
  plant_ianode_1s %>%
    select(plant, pot, ym) %>%
    group_by(plant, pot) %>%
    summarise(ym = min(ym, na.rm = T)) %>%
    slice_max(ym) %>%
    inner_join(plant_ianode_1s, by = c("plant", "pot", "ym")) %>%
    select(plant, pot, ts) %>%
    group_by(plant, pot) %>%
    summarize(min_ts = min(ts, na.rm = T)) %>%
    ungroup() %>%
    arrange(pot) %>%
    collect() %>%
    mutate(min_ts = as.POSIXct(min_ts, tz="UTC", origin="1970-01-01"))
}


# TODO: refactor year month -> ym
get_max_ts_per_pot <- function(sc, plant_code) {
  plant_ianode_1s <- tbl(sc, "ianode_1s") %>%
    filter(plant == plant_code)
  print("Get the most recent timestamp for each pot.")
  plant_ianode_1s %>%
    select(plant, pot, year, month) %>%
    group_by(plant, pot, year) %>%
    summarise(month = max(month, na.rm = T)) %>%
    slice_max(year) %>%
    inner_join(plant_ianode_1s, by = c("plant", "pot", "year", "month")) %>%
    select(plant, pot, ts) %>%
    group_by(plant, pot) %>%
    summarize(max_ts = max(ts, na.rm = T)) %>%
    mutate(pot = as.character(as.integer(pot))) %>%
    arrange(pot) %>%
    collect() %>%
    select(pot, max_ts) %>% # TODO: Shall not need to select and summarize again, fix that!
    group_by(pot) %>%
    summarize(max_ts = max(max_ts))
}


get_plants_lag_time <- function(sc, table_name = "ianode_1s") {
  ianodes <- tbl(sc, table_name) %>%
    select(plant, ym, ts)

  result_min <- ianodes %>%
    group_by(plant) %>%
    summarise(min_ym = min(ym, na.rm = T)) %>%
    ungroup() %>%
    inner_join(ianodes, by = c("plant", "min_ym" = "ym")) %>%
    select(-min_ym) %>%
    group_by(plant) %>%
    summarize(min_ts = min(ts, na.rm = T)) %>%
    ungroup()

  result_max <- ianodes %>%
    group_by(plant) %>%
    summarise(max_ym = max(ym, na.rm = T)) %>%
    ungroup() %>%
    inner_join(ianodes, by = c("plant", "max_ym" = "ym")) %>%
    select(-max_ym)%>%
    group_by(plant) %>%
    summarize(max_ts = max(ts, na.rm = T)) %>%
    ungroup()

  result_min %>%
    inner_join(result_max, by = "plant") %>%
    collect()
}


get_plants_last_extract <- function(sc, table_name = "ianode_1s") {
  ianodes <- tbl(sc, table_name) %>% select(plant, ym, ts)
  ianodes %>%
    group_by(plant) %>%
    summarise(min_ym = min(ym, na.rm = T)) %>%
    ungroup() %>%
    inner_join(ianodes, by = c("plant", "min_ym" = "ym")) %>%
    select(-min_ym) %>%
    group_by(plant) %>%
    summarize(first_ts = min(ts, na.rm = T)) %>%
    ungroup() %>%
    collect()
}


create_intervals <- function(plant_min_ts_per_pot, plant_tags, plants_last_extract, nday = 1, period_days = 1) {
  plant_min_ts_per_pot %>%
    right_join(plant_tags, by = c("plant", "pot")) %>%
    inner_join(plants_last_extract, by = "plant") %>%
    mutate(start_time = ceiling_date(coalesce(min_ts, first_ts) - ddays(nday), "day"),
           end_time = (floor_date(start_time + ddays(period_days), "day")),
           sync_time = start_time) %>%
    arrange(pot, tag) %>%
    select(plant, pot, tag, path, kind, start_time, end_time, sync_time)
}


extract_ianode <- function(plant, extraction_intervals) {
  data_buffer <- paste0("hdfs://casagzclem1/tmp/extract_ianode/", plant)
  pot_count <<- 0
  extraction_intervals %>%
    group_by(pot) %>%
    group_walk(extract_pot_ianode, data_buffer, plant)
  data_buffer
}


save_buffer_to_spark_table <- function(data_buffer) {
  print("Moving the temporary parquet to the final Hive table.")
  spark_read_parquet(sc,
                     name = "temp_ianode",
                     path = data_buffer,
                     overwrite = T) %>%
    sdf_repartition(partition_by = c("plant", "pot", "ym")) %>%
    spark_write_table("ianode_1s", mode = "append")
  data_buffer_path <- right(data_buffer, 23)
  system(paste0("hdfs dfs -rm -R ", data_buffer_path))
}


extract_pot_ianode <- function(.x, .y, data_buffer, plant) {

  i <<- 0
  pot <- .y$pot
  pot_tags <- as.data.frame(.x)
  tag_count <- nrow(pot_tags)
  rownames(pot_tags) <- pot_tags$path

  rowcount <- 0
  while (rowcount == 0 && nrow(pot_tags) > 0 && pot_tags$end_time[[1]] <= today()) {

    print(paste0("Extraction for pot ", pot, "; From ", pot_tags$start_time[[1]], " to ",
                 pot_tags$end_time[[1]]))

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

    rowcount <- nrow(pot_data)
    if (rowcount == 0) {
      i <<- 0
      pot_tags <- pot_tags %>% mutate(end_time = end_time + ddays(1))
      print(paste("No data, increment end_time to", pot_tags$end_time[[1]]))
    }
  }

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
    inner_join(pot_tags, by = "tag") %>%
    transmute(pot,
              ym = as.integer(year(ts) * 100 + month(ts)),
              ts = floor_date(ts, "seconds"),
              kind,
              val = numval) %>%
    pivot_wider(id_cols = c("pot", "ym", "ts"),
                names_from = kind,
                values_from =  val,
                values_fn = first) %>%
    filter(ts > placeholder_ts)


  if (plant == "AAR") {
    pivoted_pot_data <- pivoted_pot_data %>%
      transmute(as.POSIXct(ts),
                iano01, iano02, iano03, iano04, iano05, iano06, iano07, iano08, iano09, iano10,
                iano11, iano12, iano13, iano14, iano15, iano16, iano17, iano18, iano19, iano20,
                iano21, iano22, iano23, iano24, i, v, plant="AAR", pot, ym)
  }
  else if (plant == "ALM") {
    pivoted_pot_data <- pivoted_pot_data %>%
      transmute(as.POSIXct(ts),
                iano01, iano02, iano03, iano04, iano05, iano06, iano07, iano08, iano09, iano10,
                iano11, iano12, iano13, iano14, iano15, iano16, iano17, iano18, iano19, iano20,
                iano21=NA, iano22=NA, iano23=NA, iano24=NA, i, v, plant="ALM", pot, ym)
  }


  print(paste0("Saving ", nrow(pivoted_pot_data), " rows."))
  sdf_pivoted_pot_data <- copy_to(sc,
                                  df = pivoted_pot_data,
                                  name = "pivoted_pot_data",
                                  overwrite = T)

  sdf_pivoted_pot_data %>%
    spark_write_parquet(data_buffer, mode = "append")


  pot_count <<- pot_count + 1
  if (pot_count %% 8 == 0) {
    print("Refresh spark session.")
    spark_disconnect(sc)
    success <- FALSE
    retry <- 0
    while (!success && retry < 3) {
      success <- tryCatch({
        sc <<- ConnectToSpark("reduction")
        TRUE
      },
      error = function(e) {
        warning(paste0("Connection to spark failed. ", e))
        FALSE
      })
      retry <- retry + 1
    }
    if (retry == 3) {
      stop("Unable to connect to spark after 3 tries.")
    }
  }

}
