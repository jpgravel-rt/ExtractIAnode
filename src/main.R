library(sparklyr)
library(reticulate)
library(lubridate)
library(logger)
library(dplyr)
library(tidyr)

source("src/helper.R")
source("src/extract_ianode.R")



pexlib <- import("pexlib")
req <- import("requests")
req$packages$urllib3$disable_warnings() 
rm(req)


client <- pexlib$Client(
  "https://casagszwebpi1.corp.riotinto.org/piwebapi/",
  "CORP\\crda.courtoisie",
  "#:3EmSY/4wKDfRBurD"                # Change me!
)
client$open()
sc <- ConnectToSpark("reduction")

tryCatch({
  
  log_info("Extraction start.")
  
  end_date <- as.Date(as.character(today()), tz = "UTC")
  plants <- c("AAR") #plants <- c("AAR", "ALM")
  
  # Extract for each plant
  lapply(plants, function(plant) {
    
    log_info("Extraction for {plant}.")
    max_ts_per_pot <- get_max_ts_per_pot(sc, plant) %>% filter(max_ts < end_date)
    if (nrow(max_ts_per_pot) == 0) {
      return(0) # there is nothing to extract
    }
    
    plant_tags <- get_tags(plant)
    full_extraction_intervals <- create_intervals(max_ts_per_pot, plant_tags, end_date) %>%
      mutate(yw = year(start_time) * 100 + week(start_time))
    
    weekly_extraction_intervals <- full_extraction_intervals %>% group_by(yw)
    group_walk(weekly_extraction_intervals, function(.x, .y) {
      extraction_intervals <- as.data.frame(.x)
      extract_ianode(plant, extraction_intervals) %>%
        save_buffer_to_spark_table()
    })

  }) %>%
    invisible()
  

},
finally = {
  client$close()
  spark_disconnect(sc)
  #spark_disconnect_all()
  log_info("Extraction done.")
})






