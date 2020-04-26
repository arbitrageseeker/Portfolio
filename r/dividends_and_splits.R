library(tidyverse)
library(glue)
library(lubridate)
library(readxl)
library(tidyquant)

eod_api_key <- read_lines("eod_api_key.txt")
in_dir <- readLines("in_dir.txt")
selenium_ticker_name <- readLines("selenium_ticker_name.txt")
merged_ticker_name <- readLines("merged_ticker_name.txt")

# tickers from transactions data ####

transactions <- read_rds(str_c(in_dir, "data/transactions.rds")) 

tickers_vec <- transactions %>% 
  filter(financial_institution != "Seligson") %>% 
  distinct(ticker) %>% 
  filter(!ticker %in% c(str_c(selenium_ticker_name, ".HE"))) %>% 
  pluck("ticker")

min_date <- transactions %>% 
  filter(transaction_date == min(transaction_date)) %>% 
  pluck("transaction_date")

# splits ####

read_splits <- function (ticker) {
  url <- glue(str_c("https://eodhistoricaldata.com/api/splits/", ticker, "?api_token={eod_api_key}"))
  
  read_lines(url) %>% 
    head(n = -1) %>% 
    read_csv(col_types = cols(.default = "c")) %>% 
    mutate(ticker = ticker)
}

splits_raw <- map(tickers_vec, safely(read_splits))

splits <- map(splits_raw, ~.x$result) %>% 
  bind_rows(.id = "id") %>% 
  transmute(transaction_date = parse_date(Date, format = "%Y-%m-%d"),
            ticker = ticker,
            split = 1 / (str_extract(`Stock Splits`, ".*(?=\\/)") %>% parse_double() /
                           str_extract(`Stock Splits`, "(?<=\\/).*") %>% parse_double()))

# dividends ####

read_dividends <- function (ticker) {
  url <- glue(str_c("https://eodhistoricaldata.com/api/div/", ticker, "?api_token={eod_api_key}"))
  
  read_lines(url) %>%
    head(n = -1) %>%
    read_csv(col_types = cols(.default = "c")) %>%
    mutate(ticker = ticker)
}

dividends_raw <- map(tickers_vec, safely(read_dividends))

dividends <- map(dividends_raw, ~.x$result) %>% 
  bind_rows(.id = "id") %>% 
  transmute(transaction_date = parse_date(Date, format = "%Y-%m-%d"),
            ticker = ticker,
            dividend = parse_double(Dividends))

# save ####

## splits ####

splits_old <- read_rds(str_c(in_dir, "data/splits.rds"))

splits_new <- splits %>% 
  anti_join(splits_old, by = c("transaction_date", "ticker"))

splits_to_save <- bind_rows(splits_old, splits_new)

write_rds(splits_to_save, str_c(in_dir, "data/splits.rds"))

## dividends ####

dividends_old <- read_rds(str_c(in_dir, "data/dividends.rds"))

dividends_new <- dividends %>% 
  anti_join(dividends_old, by = c("transaction_date", "ticker"))

dividends_to_save <- bind_rows(dividends_old, dividends_new)

write_rds(dividends_to_save, str_c(in_dir, "data/dividends.rds"))
