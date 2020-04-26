library(tidyverse)
library(lubridate)
library(readxl)
library(glue)

in_dir <- readLines("in_dir.txt")
eod_api_key <- read_lines("eod_api_key.txt")
quandl_api_key(readLines("quandl_api_key.txt"))
selenium_ticker_name <- readLines("selenium_ticker_name.txt")
merged_ticker_name <- readLines("merged_ticker_name.txt")

transactions <- read_rds(str_c(in_dir, "data/transactions.rds"))

tickers_vec <- transactions %>% 
  filter(financial_institution != "Seligson") %>% 
  distinct(ticker) %>% 
  filter(!ticker %in% c(str_c(selenium_ticker_name, ".HE"))) %>% 
  pluck("ticker")

min_date <- transactions %>% 
  filter(transaction_date == min(transaction_date)) %>% 
  pluck("transaction_date")

# processing currency data ####

currencies_raw <- tq_get(c("ECB/EURUSD", "ECB/EURGBP"),
                         get = "quandl",
                         from = "1970-01-01",
                         to = Sys.Date())

currencies <- currencies_raw %>% 
  transmute(date,
            currency = case_when(
              str_remove_all(symbol, "ECB/EUR") == "GBP" ~ "GBX",
              TRUE ~ str_remove_all(symbol, "ECB/EUR")),
            market_exchange_rate = case_when(
              currency == "GBX" ~ (1/value)/100,
              TRUE ~ 1/value))

currencies_old <- read_rds(str_c(in_dir, "data/currencies.rds"))

currencies_new <- currencies %>% 
  anti_join(currencies_old, by = c("date", "currency"))

currencies_to_save <- bind_rows(currencies_old, currencies_new)

write_rds(currencies_to_save, str_c(in_dir, "data/currencies.rds"))

# processing stock and fund prices

## stock prices ####

selenium_ticker <- read_rds(str_c(in_dir, "data/", selenium_ticker_name, ".rds"))

read_daily_data <- function (ticker) {
  url <- glue(str_c("https://eodhistoricaldata.com/api/eod/", ticker, "?api_token={eod_api_key}"))
  
  read_lines(url) %>% 
    head(n = -1) %>% 
    read_csv(col_types = cols(.default = "c")) %>% 
    mutate(ticker = ticker)
}

df_daily_raw <- map(tickers_vec, safely(read_daily_data))

df_daily <- map(df_daily_raw, ~.x$result) %>% 
  bind_rows(.id = "id") %>% 
  transmute(date = parse_date(Date, format = "%Y-%m-%d"),
            ticker = ticker,
            opening_price = parse_double(Open),
            opening_adjusted_price = opening_price,
            highest_price = parse_double(High),
            highest_adjusted_price = highest_price,
            lowest_price = parse_double(Low),
            lowest_adjusted_price = lowest_price,
            closing_price = parse_double(Close),
            closing_adjusted_price = closing_price,
            adjusted_dividends_price = parse_double(Adjusted_close),
            trading_volume = parse_double(Volume),
            trading_volume_adjusted = trading_volume,
            turnover = trading_volume * closing_price)

read_live_data <- function (ticker) {
  url <- glue(str_c("https://eodhistoricaldata.com/api/real-time/", ticker, "?api_token={eod_api_key}&fmt=csv"))
  
  read_lines(url) %>% 
    head(n = -1) %>% 
    read_csv(col_types = cols(.default = "c")) %>% 
    mutate(ticker = ticker)
}

df_live_raw <- map(tickers_vec, safely(read_live_data))

df_live <- map(df_live_raw, ~.x$result) %>% 
  bind_rows(.id = "id") %>% 
  transmute(timestamp = as.POSIXct(timestamp %>% parse_integer(), origin = "1970-01-01"),
            date = as.Date(timestamp),
            ticker = ticker,
            opening_price = parse_double(open),
            opening_adjusted_price = opening_price,
            highest_price = parse_double(high),
            highest_adjusted_price = highest_price,
            lowest_price = parse_double(low),
            lowest_adjusted_price = lowest_price,
            closing_price = parse_double(close),
            closing_adjusted_price = closing_price,
            adjusted_dividends_price = parse_double(close),
            trading_volume = parse_double(volume),
            trading_volume_adjusted = trading_volume,
            turnover = trading_volume * closing_price) %>% 
  filter(!is.na(opening_price)) %>% 
  anti_join(df_daily, by = c("date", "ticker"))


stock_prices <- df_daily %>% 
  bind_rows(df_live) %>% 
  bind_rows(selenium_ticker)

## fund prices ####

tickers_seligson <- transactions %>% 
  filter(financial_institution == "Seligson") %>% 
  distinct(ticker)

fund_urls <- tickers_seligson %>% 
  mutate(url = str_c("https://www.seligson.fi/graafit/", ticker, "_exc.csv")) %>% 
  pluck("url")

seligson <- tibble(fund = fund_urls) %>%
  mutate(fund_data = map(fund, ~read_delim(.x, col_names = F,
                                                 delim = ";",
                                                 locale = locale(decimal_mark = ","),
                                                 col_types = cols(.default = "c")))) %>% 
  unnest(fund_data) %>% 
  transmute(date = dmy(X1),
            ticker = str_extract(fund, "(?<=graafit\\/).*(?=_)"),
            closing_price = parse_double(str_replace_all(X2, "\\,", "\\.")),
            closing_adjusted_price = closing_price,
            adjusted_dividends_price = closing_price)

# combine and save ####

stock_and_fund_prices <- bind_rows(stock_prices, seligson)

stock_and_fund_prices_old <- read_rds(str_c(in_dir, "data/stock_and_fund_prices.rds")) %>% 
  group_by(ticker) %>% 
  mutate(ticker_max_date = max(date)) %>% 
  ungroup()

tickers_max_dates <- stock_and_fund_prices_old %>% 
  distinct(ticker, ticker_max_date)

stock_and_fund_prices_new <- stock_and_fund_prices %>% 
  anti_join(stock_and_fund_prices_old, by = c("date", "ticker")) %>% 
  left_join(tickers_max_dates, by = "ticker") %>% 
  filter(date > ticker_max_date)

stock_and_fund_prices_to_save <- bind_rows(stock_and_fund_prices_old, stock_and_fund_prices_new) 

write_rds(stock_and_fund_prices_to_save, str_c(in_dir, "data/stock_and_fund_prices.rds"))
