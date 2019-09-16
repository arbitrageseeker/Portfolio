library(tidyverse)
library(lubridate)
library(readxl)
library(tidyquant)

in_dir <- readLines("in_dir.txt")
quandl_api_key(readLines("quandl_api_key.txt"))
selenium_ticker_name <- readLines("selenium_ticker_name.txt")
merged_ticker_name <- readLines("merged_ticker_name.txt")

# tickers from transactions data ####

transactions <- read_rds(str_c(in_dir, "/transactions.rds")) 

tickers_vec <- transactions %>% 
  filter(financial_institution != "Seligson") %>% 
  distinct(ticker) %>% 
  filter(!ticker %in% c(merged_ticker_name, str_c(selenium_ticker_name, ".HE"))) %>% 
  pluck("ticker")

min_date <- transactions %>% 
  filter(transaction_date == min(transaction_date)) %>% 
  pluck("transaction_date")

# splits ####

splits <- tq_get(tickers_vec,
                 get = "splits",
                 from = min_date,
                 to = Sys.Date()) %>% 
  transmute(transaction_type = "split",
            transaction_date = date,
            ticker = symbol,
            split = splits)

# dividends ####

dividends_merged_ticker <- tq_get(str_c("WIKI/", merged_ticker_name),
                             get = "quandl",
                             from = min_date,
                             to = Sys.Date()) %>%
  transmute(transaction_type = "dividend",
            transaction_date = date,
            ticker = merged_ticker_name,
            dividend_ex = ex.dividend) %>% 
  filter(dividend_ex > 0)

dividends <- tq_get(tickers_vec,
                    get = "dividends",
                    from = min_date,
                    to = Sys.Date()) %>% 
  transmute(transaction_type = "dividend",
            transaction_date = date,
            ticker = symbol,
            dividend_ex = dividends) %>% 
  bind_rows(dividends_merged_ticker)

# save ####

## splits ####

splits_old <- read_rds(str_c(in_dir, "/splits.rds"))

splits_new <- splits %>% 
  anti_join(splits_old, by = c("transaction_type", "transaction_date", "ticker"))

splits_to_save <- bind_rows(splits_old, splits_new)

write_rds(splits_to_save, str_c(in_dir, "/splits.rds"))

## dividends ####

dividends_old <- read_rds(str_c(in_dir, "/dividends.rds"))

dividends_new <- dividends %>% 
  anti_join(dividends_old, by = c("transaction_type", "transaction_date", "ticker"))

dividends_to_save <- bind_rows(dividends_old, dividends_new)

write_rds(dividends_to_save, str_c(in_dir, "/dividends.rds"))
