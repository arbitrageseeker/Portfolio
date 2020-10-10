library(tidyverse)
library(glue)
library(lubridate)
library(readxl)
library(aws.s3)

eod_api_key <- Sys.getenv("eod_api_key")
selenium_ticker_name <- Sys.getenv("selenium_ticker_name")
merged_ticker_name <- Sys.getenv("merged_ticker_name")

# tickers from transactions data ####

transactions <- s3read_using(FUN = read_rds, bucket = Sys.getenv("bucket"),
                             object = "transactions.rds")

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

splits_old <- s3read_using(FUN = read_rds, bucket = Sys.getenv("bucket"),
                            object = "splits.rds")

splits_new <- splits %>% 
  anti_join(splits_old, by = c("transaction_date", "ticker"))

splits_to_save <- bind_rows(splits_old, splits_new)

write_rds(splits_to_save, file.path(tempdir(), "splits.rds"))

put_object(
  file = file.path(tempdir(), "splits.rds"), 
  object = "splits.rds", 
  bucket = Sys.getenv("bucket")
)

## dividends ####

dividends_old <- s3read_using(FUN = read_rds, bucket = Sys.getenv("bucket"),
                           object = "dividends.rds")

dividends_new <- dividends %>% 
  anti_join(dividends_old, by = c("transaction_date", "ticker"))

dividends_to_save <- bind_rows(dividends_old, dividends_new)

write_rds(dividends_to_save, file.path(tempdir(), "dividends.rds"))

put_object(
  file = file.path(tempdir(), "dividends.rds"), 
  object = "dividends.rds", 
  bucket = Sys.getenv("bucket")
)
