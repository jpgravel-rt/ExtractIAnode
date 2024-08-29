library(reticulate)
library(dplyr)
library(lubridate)
library(sparklyr)
library(tidyr)
library(data.table)

source("src/helper.R")
source("src/extract_ianode.R")

print("IAnodeExtract Backward version 1.0")

pexlib <- import("pexlib")
req <- import("requests")
req$packages$urllib3$disable_warnings()
rm(req)

EXTRACT_PERIOD_DAYS = 7

client <- pexlib$Client(
  "https://casagszwebpi1.corp.riotinto.org/piwebapi/",
  "CORP\\crda.courtoisie",
  "#:3EmSY/4wKDfRBurD"                # Change me!
)
client$open()
sc <<- ConnectToSpark("reduction")

tryCatch({

  print("Evaluate the lag time of each plant.")
  to_ts_limit_high <- today()
  from_ts_limit_low <- as.POSIXct("2024-01-01")
  plants <- data.frame(#plant = c("ALM", "AAR"),
                       plant = c("AAR"),
                       default_from_ts = from_ts_limit_low,
                       default_to_ts = to_ts_limit_high)
  plants_last_extract <- get_plants_last_extract(sc) %>%
    right_join(plants, by = "plant") %>%
    mutate(first_ts = as.POSIXct(first_ts, tz = "UTC", origin = "1970-01-01")) %>%
    mutate(first_ts = coalesce(first_ts, default_to_ts)) %>%
    select(plant, first_ts) %>%
    arrange(plant)


  print("List of plants and their earliest extraction date.")
  print(plants_last_extract)

  lapply(plants_last_extract$plant, function(plant_id) {
    print(paste("***** PLANT", plant_id, "*****"))
    plant_last_extract <- plants_last_extract %>% filter(plant == plant_id)
    lapply(1:EXTRACT_PERIOD_DAYS, function(nb_days) {
      plant_min_ts_per_pot <- get_min_ts_per_pot(sc, plant_id)
      plant_tags <- get_tags(plant_id) %>% mutate(pot = as.integer(pot))
      extraction_intervals <- create_intervals(plant_min_ts_per_pot, plant_tags,
                                               plants_last_extract, nb_days)
      ianode_path <- extract_ianode(plant_id, extraction_intervals)
      data.frame(path = ianode_path)
    }) %>%
      bind_rows() %>%
      distinct() %>%
      save_buffer_to_spark_table()
  })


},
finally = {
  client$close()
  spark_disconnect(sc)
  spark_disconnect_all()
})
