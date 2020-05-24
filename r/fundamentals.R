library(tidyverse)
library(lubridate)
library(readxl)
library(glue)
library(httr)
library(tmaptools)

in_dir <- readLines("in_dir.txt")
eod_api_key <- read_lines("eod_api_key.txt")
selenium_ticker_name <- readLines("selenium_ticker_name.txt")

transactions <- read_rds(str_c(in_dir, "data/transactions.rds"))

tickers_vec <- transactions %>% 
  filter(financial_institution != "Seligson") %>% 
  distinct(ticker) %>% 
  filter(!ticker %in% c(str_c(selenium_ticker_name, ".HE"))) %>% 
  pluck("ticker")


read_fundamentals <- function (ticker) {
  url <- glue(str_c("https://eodhistoricaldata.com/api/fundamentals/", ticker, "?api_token={eod_api_key}"))
  
  GET(url) %>% 
    content("parsed")
}

df_raw <- map(tickers_vec, safely(read_fundamentals)) %>% 
  map(~.x$result) %>% 
  set_names(tickers_vec)

proc_general <- function (json) {
  
  json$General %>% 
    discard(is.null) %>%
    as_tibble() %>% 
    select(-Officers) %>% 
    distinct()
}

df_raw2 <- map(df_raw, proc_general) %>% 
  bind_rows(.id = "ticker") %>% 
  select(ticker, exchange = Exchange, address = Address, name = Name, exchange_country = CountryName, 
         exchange_country_iso = CountryISO, sector = Sector, industry = Industry, 
         sector_gic = GicSector, group_gic = GicGroup, industry_gic = GicIndustry, 
         subindustry_gic = GicSubIndustry, employees = FullTimeEmployees,
         updated_at = UpdatedAt) %>% 
  mutate(updated_at = parse_date(updated_at, "%Y-%m-%d"),
         postal_code = str_remove(address, ".*[\\,]") %>% str_squish(),
         state = str_extract(address, "\\s[A-Z]{1,2}\\,") %>% 
           str_extract("[A-Z]+") %>% str_squish(),
         city = str_extract(address, "(?<=\\,).*(?=\\,)") %>% 
           str_extract("[^\\,]+") %>% str_squish(),
         house_number = str_extract(address, "\\d+[\\,\\s]") %>% 
           str_extract("\\d+") %>% str_squish(),
         street = str_extract(address, "[^\\,]+") %>% str_remove_all("\\d") %>% 
           str_squish(),
         country = str_extract(address, "(?<=\\,).*(?=\\,)") %>% 
           str_remove(".*\\,") %>% str_squish())

addresses <- df_raw2 %>% 
  transmute(address1 = str_c(street, ", ", house_number, ", ", postal_code, ", ",
                             city, ", ", country),
            address2 = str_c(street, ", ", postal_code, ", ",
                             city, ", ", country),
            address3 = str_c(street, ", ", postal_code, ", ",
                             city, ", ", country),
            address4 = str_c(house_number, ", ", street, ", ", 
                            postal_code, ", ", city, ", ", replace_na(state, ""), ", ", country),
            address5 = str_c(street, ", ", postal_code, ", ",
                            city, ", ", replace_na(state, ""), ", ", country),
            address6 = str_c(postal_code, ", ",
                            city, ", ", replace_na(state, ""), ", ", country))

geocode_with_sleep <- function (address){
  Sys.sleep(1.1)
  geocode_OSM(address)$coords
}

find_while <- function(v) {
  gc_while <- function(old, addr, ind) {
    if (!is.null(old$result)) return(old)
    list(result = quietly(geocode_with_sleep)(addr)$result, ind = ind)
  }
  reduce2(v, seq_along(v), gc_while, .init = NULL)
}

coord_raw <- mutate_all(addresses, function(x) replace_na(x, "")) %>%
  pmap(c) %>% map(safely(find_while))

df_general <- coord_raw %>%
  transpose() %>%
  as_tibble() %>%
  transmute(lon = map_dbl(result, ~.x$result["x"] %||% NA_real_),
            lat = map_dbl(result, ~.x$result["y"] %||% NA_real_)) %>%
  bind_cols(df_raw2)

general_old <- read_rds(str_c(in_dir, "data/fundamentals_general.rds"))

general_new <- df_general %>% 
  anti_join(general_old, by = "ticker")

general_to_save <- bind_rows(general_old, general_new)

write_rds(general_to_save, str_c(in_dir, "data/fundamentals_general.rds"))
  
library(leaflet)

leaflet(df2) %>% 
  addTiles() %>%
  addCircleMarkers(lng = ~lon, lat = ~lat,
                   radius = 6.5,
                   stroke = FALSE, fillOpacity = 1,
                   label = map(paste0('Yritys: ', df2$name, '<p></p>',
                                      'Osoite: ', df2$address, '</p><p>'),
                               htmltools::HTML))


shinyLP::runExample()
