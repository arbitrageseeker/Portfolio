library(tidyverse)
library(lubridate)
library(readxl)
library(tidyquant)

in_dir <- readLines("in_dir.txt")
quandl_api_key(readLines("quandl_api_key.txt"))
selenium_ticker_name <- readLines("selenium_ticker_name.txt")
merged_ticker_name <- readLines("merged_ticker_name.txt")

# tickers from transactions data ####

tickers_vec <- read_rds(str_c(in_dir, "/transactions.rds")) %>% 
  filter(financial_institution != "Seligson") %>% 
  distinct(ticker) %>% 
  filter(!ticker %in% c(merged_ticker_name, str_c(selenium_ticker_name, ".HE"))) %>% 
  pluck("ticker")

# splits ####

splits <- tq_get(tickers_vec,
                 get = "splits",
                 from = "2013-06-07",
                 to = Sys.Date()) %>% 
  transmute(transaction_type = "split",
            transaction_date = date,
            ticker = symbol,
            split = splits)

# dividends ####

dividends_merged_ticker <- tq_get(str_c("WIKI/", merged_ticker_name),
                             get = "quandl",
                             from = "2013-06-07",
                             to = Sys.Date()) %>%
  transmute(transaction_type = "dividend",
            transaction_date = date,
            ticker = merged_ticker_name,
            dividend_ex = ex.dividend) %>% 
  filter(dividend_ex > 0)

dividends <- tq_get(tickers_vec,
                    get = "dividends",
                    from = "2013-06-07",
                    to = Sys.Date()) %>% 
  transmute(transaction_type = "dividend",
            transaction_date = date,
            ticker = symbol,
            dividend_ex = dividends) %>% 
  bind_rows(dividends_merged_ticker)

# combine ####

splits_and_dividends <- bind_rows(splits, dividends)

splits_and_dividends_old <- read_rds(str_c(in_dir, "/splits_and_dividends.rds"))

splits_and_dividends_new <- splits_and_dividends %>% 
  anti_join(splits_and_dividends_old, by = c("transaction_type", "transaction_date", "ticker"))

splits_and_dividends_to_save <- bind_rows(splits_and_dividends_old, splits_and_dividends_new)

write_rds(splits_and_dividends_to_save, str_c(in_dir, "/splits_and_dividends.rds"))
