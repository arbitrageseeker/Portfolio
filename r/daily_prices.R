library(tidyverse)
library(lubridate)
library(readxl)
library(tidyquant)

in_dir <- readLines("in_dir.txt")
quandl_api_key(readLines("quandl_api_key.txt"))
selenium_ticker_name <- readLines("selenium_ticker_name.txt")
merged_ticker_name <- readLines("merged_ticker_name.txt")

# processing currency data ####

currencies_raw <- tq_get(c("ECB/EURUSD", "ECB/EURGBP"),
                         get = "quandl",
                         from = min_date,
                         to = Sys.Date())

currencies <- currencies_raw %>% 
  transmute(date,
            currency = case_when(
              str_remove_all(symbol, "ECB/EUR") == "GBP" ~ "GBX",
              TRUE ~ str_remove_all(symbol, "ECB/EUR")),
            market_exchange_rate = case_when(
              currency == "GBX" ~ (1/value)/100,
              TRUE ~ 1/value))

currencies_old <- read_rds(str_c(in_dir, "/currencies.rds"))

currencies_new <- currencies %>% 
  anti_join(currencies_old, by = c("date", "currency"))

currencies_to_save <- bind_rows(currencies_old, currencies_new)

write_rds(currencies_to_save, str_c(in_dir, "/currencies.rds"))

# processing stock and fund prices

## stock prices ####

transactions <- read_rds(str_c(in_dir, "/transactions.rds"))

tickers_vec <- transactions %>% 
  filter(financial_institution != "Seligson") %>% 
  distinct(ticker) %>% 
  filter(!ticker %in% c(merged_ticker_name, str_c(selenium_ticker_name, ".HE"))) %>% 
  pluck("ticker")

min_date <- transactions %>% 
  filter(date == min(date)) %>% 
  pluck("date")

selenium_ticker <- read_rds(str_c(in_dir, "/", selenium_ticker_name, ".rds"))

reynolds_prices <- tq_get(str_c("WIKI/", merged_ticker_name),
                          get = "quandl",
                          from = min_date,
                          to = Sys.Date()) %>% 
  transmute(date,
            ticker = merged_ticker_name,
            closing_price = close)

stock_prices <- tq_get(tickers_vec,
                       get = "stock.prices",
                       from = min_date,
                       to = Sys.Date()) %>% 
  transmute(date,
            ticker = symbol,
            closing_price = close) %>% 
  bind_rows(selenium_ticker) %>% 
  bind_rows(reynolds_prices) 

## fund prices ####

tickers_seligson <- tickers %>% 
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
  unnest() %>% 
  transmute(date = dmy(X1),
            ticker = str_extract(fund, "(?<=graafit\\/).*(?=_)"),
            closing_price = parse_double(str_replace_all(X2, "\\,", "\\.")))


# combine and save ####

stock_and_fund_prices <- bind_rows(stock_prices, seligson)

stock_and_fund_prices_old <- read_rds(str_c(in_dir, "/stock_and_fund_prices.rds"))

stock_and_fund_prices_new <- stock_and_fund_prices %>% 
  anti_join(stock_and_fund_prices_old, by = c("date", "ticker"))

stock_and_fund_prices_to_save <- bind_rows(stock_and_fund_prices_old, stock_and_fund_prices_new)

write_rds(stock_and_fund_prices_to_save, str_c(in_dir, "/stock_and_fund_prices.rds"))

