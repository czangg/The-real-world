############################################
####      Adding additional channels    ####
############################################


library(jsonlite)
library(dplyr)
library(purrr)
library(tidytext)
library(tm)
library(stringr)
library(readxl)

rm(list=ls()) ##Clean up workspace

setwd("C:/Data/The-real-world/")
main.path <- getwd()

#Load existing files
df.matched <- read.csv("./Code/real_world1_matched.csv")
ch.users <- read.csv("./Code/CH_users.csv")

#rename some vars
df.matched <- df.matched %>%
  rename(id_chat = id.y,
         content_chat = content.y)

#Define a function such that we do not need to call this over and over again:

read_and_append_json <- function(json_path, user_data, existing_df) {
  # Read JSON file
  json_data <- stream_in(file(json_path), simplifyDataFrame = FALSE)
  
  # Parse nested structure
  parsed_df <- map_df(json_data, function(batch) {
    map_df(batch, function(x) {
      tibble(
        id = x$`_id` %||% NA_character_,
        channel = x$channel %||% NA_character_,
        author = x$author %||% NA_character_,
        content = x$content %||% NA_character_
      )
    })
  })
  
  # Inner join with user base
  matched_df <- user_data %>%
    inner_join(parsed_df, by = c("id" = "author"))
  matched_df <- matched_df %>%
    rename(id_chat = id.y,
           content_chat = content.y)
  
  # Append to existing dataframe
  updated_df <- rbind(existing_df, matched_df)
  
  return(updated_df)
}


### Add additional channels

# Get all JSON files in the folder
json_files <- list.files(
  path = "C:/Data/The-real-world/Data/Private/The Real World",
  pattern = "\\.json$",
  full.names = TRUE
)

#remove the one already read in
json_files <- json_files[!grepl("01H261HJTJ4K6RM2WWGW5PS40E", json_files)]

# Loop over all files
for (file_path in json_files) {
  df.matched <- read_and_append_json(
    json_path = file_path,
    user_data = ch.users,
    existing_df = df.matched
  )
}

write.csv(df.matched, file=paste0(main.path, "/Code/real_world_all_matched.csv"))

