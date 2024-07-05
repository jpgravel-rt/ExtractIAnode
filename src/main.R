library(reticulate)
library(dplyr)
library(lubridate)
library(sparklyr)
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
sc <<- ConnectToSpark("reduction")

tryCatch({
  
  print("Evaluate the lag time of each plant.")
  extraction_time_limit <- as.POSIXct(today())
  plants <- data.frame(plant = c("AAR", "ALM"))
  plants_lag_time <- get_plants_lag_time() %>%
    full_join(plants, by = "plant") %>%
    replace_na(list(max_ts = as.POSIXct("2019-01-01"))) %>%
    filter((extraction_time_limit - max_ts) >= ddays(1))
  
  print("List of plants and their lag. Up to date plants are not shown.")
  print(plants_lag_time)
  
  if (count(plants_lag_time) > 0) {
    last_date <- plants_lag_time$max_ts %>% min()
    while (extraction_time_limit - last_date > dseconds(1)) {
      last_date_df <- lapply(plants_lag_time$plant, function(plant) {
        plant_max_ts <- get_max_ts_per_pot(sc, plant) %>%
          filter((extraction_time_limit - max_ts) >= ddays(1))
        
        from_dates <- plant_max_ts %>% select(pot, max_ts) %>% distinct() %>% arrange()
        print(paste(plant, from_dates$pot, from_dates$max_ts))
        
        plant_tags <- get_tags(plant)
        extraction_intervals <- create_intervals(plant_tags, plant_max_ts, 1) %>%
          filter(end_time <= extraction_time_limit)
        
        print(system.time({
          extract_ianode(plant, extraction_intervals)
        }))
        
        # returns the earliest end time
        extraction_intervals %>%
          transmute(plant=plant, end_time) %>%
          summarize(end_time = min(end_time))
      }) %>%
        bind_rows() %>%
        summarize(end_time = min(end_time))
      last_date <- last_date_df$end_time
    }
  }
  
},
finally = {
  client$close()
  spark_disconnect(sc)
  #spark_disconnect_all()
})






