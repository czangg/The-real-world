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

if (FALSE){ #already run this whole block of code

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

df.matched$parent.directory <- "Private: The real world"

df.matched <- df.matched %>%
  select(-X.1)


write.csv(df.matched, file=paste0(main.path, "/Code/real_world_all_matched.csv"))



### Health and Fitness (Private)

rm(list = ls())


setwd("C:/Data/The-real-world/")
main.path <- getwd()

#Load existing files
df.matched <- read.csv("./Code/real_world_all_matched.csv")
ch.users <- read.csv("./Code/CH_users.csv")

df.matched <- df.matched %>%
  select(-X.1)


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
  matched_df <- matched_df %>% 
    mutate(parent.directory="Private: Health & Fitness")
  
  
  # Append to existing dataframe
  updated_df <- rbind(existing_df, matched_df)
  
  return(updated_df)
}

# Get all JSON files in the folder
json_files <- list.files(
  path = "C:/Data/The-real-world/Data/Private/Health & Fitness/",
  pattern = "\\.json$",
  full.names = TRUE
)



# Loop over all files
for (file_path in json_files) {
  df.matched <- read_and_append_json(
    json_path = file_path,
    user_data = ch.users,
    existing_df = df.matched
  )
}

write.csv(df.matched, file=paste0(main.path, "/Code/real_world_all_matched2.csv"))


### Hustler's Campus (Private)

rm(list = ls())


setwd("C:/Data/The-real-world/")
main.path <- getwd()

#Load existing files
df.matched <- read.csv("./Code/real_world_all_matched2.csv")
ch.users <- read.csv("./Code/CH_users.csv")

df.matched <- df.matched %>%
  select(-X.1)

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
  matched_df <- matched_df %>% 
    mutate(parent.directory="Private: Hustler's Campus")
  
  
  # Append to existing dataframe
  updated_df <- rbind(existing_df, matched_df)
  
  return(updated_df)
}

# Get all JSON files in the folder
json_files <- list.files(
  path = "C:/Data/The-real-world/Data/Private/Hustler's Campus/",
  pattern = "\\.json$",
  full.names = TRUE
)



# Loop over all files
for (file_path in json_files) {
  df.matched <- read_and_append_json(
    json_path = file_path,
    user_data = ch.users,
    existing_df = df.matched
  )
}

write.csv(df.matched, file=paste0(main.path, "/Code/real_world_all_matched3.csv"))



###Social Media & Client Acquisition (Private)


rm(list = ls())


setwd("C:/Data/The-real-world/")
main.path <- getwd()

#Load existing files
df.matched <- read.csv("./Code/real_world_all_matched3.csv")
ch.users <- read.csv("./Code/CH_users.csv")

df.matched <- df.matched %>%
  select(-X.1)

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
  matched_df <- matched_df %>% 
    mutate(parent.directory="Private: Social Media & Client Acquisition")
  
  
  # Append to existing dataframe
  updated_df <- rbind(existing_df, matched_df)
  
  return(updated_df)
}

# Get all JSON files in the folder
json_files <- list.files(
  path = "C:/Data/The-real-world/Data/Private/Social Media & Client Acquisition/",
  pattern = "\\.json$",
  full.names = TRUE
)



# Loop over all files
for (file_path in json_files) {
  df.matched <- read_and_append_json(
    json_path = file_path,
    user_data = ch.users,
    existing_df = df.matched
  )
}

write.csv(df.matched, file=paste0(main.path, "/Code/real_world_all_matched4.csv"))



### Ecommerce (Private)


rm(list = ls())


setwd("C:/Data/The-real-world/")
main.path <- getwd()

#Load existing files
df.matched <- read.csv("./Code/real_world_all_matched4.csv")
ch.users <- read.csv("./Code/CH_users.csv")

df.matched <- df.matched %>%
  select(-X.1)

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
  matched_df <- matched_df %>% 
    mutate(parent.directory="Private: Ecommerce")
  
  
  # Append to existing dataframe
  updated_df <- rbind(existing_df, matched_df)
  
  return(updated_df)
}

# Get all JSON files in the folder
json_files <- list.files(
  path = "C:/Data/The-real-world/Data/Private/Ecommerce/",
  pattern = "\\.json$",
  full.names = TRUE
)



# Loop over all files
for (file_path in json_files) {
  df.matched <- read_and_append_json(
    json_path = file_path,
    user_data = ch.users,
    existing_df = df.matched
  )
}

write.csv(df.matched, file=paste0(main.path, "/Code/real_world_all_matched5.csv"))


### Cryptocurrency Investing (Private)

rm(list = ls())


setwd("C:/Data/The-real-world/")
main.path <- getwd()

#Load existing files
df.matched <- read.csv("./Code/real_world_all_matched5.csv")
ch.users <- read.csv("./Code/CH_users.csv")

df.matched <- df.matched %>%
  select(-X.1)

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
  matched_df <- matched_df %>% 
    mutate(parent.directory="Private: Cryptocurrency Investing")
  
  
  # Append to existing dataframe
  updated_df <- rbind(existing_df, matched_df)
  
  return(updated_df)
}

# Get all JSON files in the folder
json_files <- list.files(
  path = "C:/Data/The-real-world/Data/Private/Cryptocurrency Investing/",
  pattern = "\\.json$",
  full.names = TRUE
)



# Loop over all files
for (file_path in json_files) {
  df.matched <- read_and_append_json(
    json_path = file_path,
    user_data = ch.users,
    existing_df = df.matched
  )
}

write.csv(df.matched, file=paste0(main.path, "/Code/real_world_all_matched6.csv"))




### Crypto Trading (Private)
rm(list = ls())


setwd("C:/Data/The-real-world/")
main.path <- getwd()

#Load existing files
df.matched <- read.csv("./Code/real_world_all_matched6.csv")
ch.users <- read.csv("./Code/CH_users.csv")

df.matched <- df.matched %>%
  select(-X.1)

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
  matched_df <- matched_df %>% 
    mutate(parent.directory="Private: Crypto Trading")
  
  
  # Append to existing dataframe
  updated_df <- rbind(existing_df, matched_df)
  
  return(updated_df)
}

# Get all JSON files in the folder
json_files <- list.files(
  path = "C:/Data/The-real-world/Data/Private/Crypto Trading/",
  pattern = "\\.json$",
  full.names = TRUE
)



# Loop over all files
for (file_path in json_files) {
  df.matched <- read_and_append_json(
    json_path = file_path,
    user_data = ch.users,
    existing_df = df.matched
  )
}

write.csv(df.matched, file=paste0(main.path, "/Code/real_world_all_matched7.csv"))


}

### AI Automation Agency (Private)
rm(list = ls())


setwd("C:/Data/The-real-world/")
main.path <- getwd()

#Load existing files
df.matched <- read.csv("./Code/real_world_all_matched7.csv")
ch.users <- read.csv("./Code/CH_users.csv")

df.matched <- df.matched %>%
  select(-X.1)

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
  matched_df <- matched_df %>% 
    mutate(parent.directory="Private: AI Automation Agency")
  
  
  # Append to existing dataframe
  updated_df <- rbind(existing_df, matched_df)
  
  return(updated_df)
}

# Get all JSON files in the folder
json_files <- list.files(
  path = "C:/Data/The-real-world/Data/Private/AI Automation Agency/",
  pattern = "\\.json$",
  full.names = TRUE
)

# Loop over all files
for (file_path in json_files) {
  df.matched <- read_and_append_json(
    json_path = file_path,
    user_data = ch.users,
    existing_df = df.matched
  )
}

write.csv(df.matched, file=paste0(main.path, "/Code/real_world_all_matched8.csv"))




### Crypto DeFi (Private)
rm(list = ls())


setwd("C:/Data/The-real-world/")
main.path <- getwd()

#Load existing files
df.matched <- read.csv("./Code/real_world_all_matched8.csv")
ch.users <- read.csv("./Code/CH_users.csv")

df.matched <- df.matched %>%
  select(-X.1)

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
  matched_df <- matched_df %>% 
    mutate(parent.directory="Private: Crypto DeFi")
  
  
  # Append to existing dataframe
  updated_df <- rbind(existing_df, matched_df)
  
  return(updated_df)
}

# Get all JSON files in the folder
json_files <- list.files(
  path = "C:/Data/The-real-world/Data/Private/Crypto DeFi/",
  pattern = "\\.json$",
  full.names = TRUE
)

# Loop over all files
for (file_path in json_files) {
  df.matched <- read_and_append_json(
    json_path = file_path,
    user_data = ch.users,
    existing_df = df.matched
  )
}

write.csv(df.matched, file=paste0(main.path, "/Code/real_world_all_matched9.csv"))




### Content Creation + AI Campus (Private)

rm(list = ls())


setwd("C:/Data/The-real-world/")
main.path <- getwd()

#Load existing files
df.matched <- read.csv("./Code/real_world_all_matched9.csv")
ch.users <- read.csv("./Code/CH_users.csv")

df.matched <- df.matched %>%
  select(-X.1)

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
  matched_df <- matched_df %>% 
    mutate(parent.directory="Private: Content Creation + AI Campus")
  
  
  # Append to existing dataframe
  updated_df <- rbind(existing_df, matched_df)
  
  return(updated_df)
}

# Get all JSON files in the folder
json_files <- list.files(
  path = "C:/Data/The-real-world/Data/Private/Content Creation + AI Campus/",
  pattern = "\\.json$",
  full.names = TRUE
)

# Loop over all files
for (file_path in json_files) {
  df.matched <- read_and_append_json(
    json_path = file_path,
    user_data = ch.users,
    existing_df = df.matched
  )
}

write.csv(df.matched, file=paste0(main.path, "/Code/real_world_all_matched10.csv"))



### Business Mastery (Private)

rm(list = ls())


setwd("C:/Data/The-real-world/")
main.path <- getwd()

#Load existing files
df.matched <- read.csv("./Code/real_world_all_matched10.csv")
ch.users <- read.csv("./Code/CH_users.csv")

df.matched <- df.matched %>%
  select(-X.1)

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
  matched_df <- matched_df %>% 
    mutate(parent.directory="Private: Business Mastery")
  
  
  # Append to existing dataframe
  updated_df <- rbind(existing_df, matched_df)
  
  return(updated_df)
}

# Get all JSON files in the folder
json_files <- list.files(
  path = "C:/Data/The-real-world/Data/Private/Business Mastery/",
  pattern = "\\.json$",
  full.names = TRUE
)

# Loop over all files
for (file_path in json_files) {
  df.matched <- read_and_append_json(
    json_path = file_path,
    user_data = ch.users,
    existing_df = df.matched
  )
}

write.csv(df.matched, file=paste0(main.path, "/Code/real_world_all_matched11.csv"))



### Copywriting (Private)

rm(list = ls())


setwd("C:/Data/The-real-world/")
main.path <- getwd()

#Load existing files
df.matched <- read.csv("./Code/real_world_all_matched11.csv")
ch.users <- read.csv("./Code/CH_users.csv")

df.matched <- df.matched %>%
  select(-X.1)

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
  matched_df <- matched_df %>% 
    mutate(parent.directory="Private: Copywriting")
  
  
  # Append to existing dataframe
  updated_df <- rbind(existing_df, matched_df)
  
  return(updated_df)
}

# Get all JSON files in the folder
json_files <- list.files(
  path = "C:/Data/The-real-world/Data/Private/Copywriting/",
  pattern = "\\.json$",
  full.names = TRUE
)

# Loop over all files
for (file_path in json_files) {
  df.matched <- read_and_append_json(
    json_path = file_path,
    user_data = ch.users,
    existing_df = df.matched
  )
}

write.csv(df.matched, file=paste0(main.path, "/Code/private_all_matched.csv"))


### Clean up

file.remove(paste0("./Code/real_world_all_matched", 2:11, ".csv"))
file.remove("./Code/real_world_all_matched.csv")

