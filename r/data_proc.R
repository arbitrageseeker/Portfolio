library(tidyverse)
library(lubridate)

in_dir <- readLines("in_dir.txt")

# sources ####

transactions <- read_rds(str_c(in_dir, "data/transactions.rds")) 

min_date <- transactions %>% 
  filter(transaction_date == min(transaction_date)) %>% 
  pluck("transaction_date")

splits <- read_rds(str_c(in_dir, "data/splits.rds")) %>% 
  mutate(transaction_type = "split")

dividends <- read_rds(str_c(in_dir, "data/dividends.rds")) %>% 
  mutate(transaction_type = "dividend")

transactions_total <- transactions %>% 
  bind_rows(dividends) %>% 
  bind_rows(splits)

stock_and_fund_prices <- read_rds(str_c(in_dir, "data/stock_and_fund_prices.rds")) 

currencies <- read_rds(str_c(in_dir, "data/currencies.rds"))

# processing data from different sources ####

initialise_dates <- function () {
  tibble(date = seq(from = min_date, to = today(), by = "day")) %>% 
    crossing(distinct(transactions_total, ticker))
}

join_transactions <- function (df) {
  df %>%
    left_join(transactions_total, by = c("date" = "transaction_date", "ticker")) %>% 
    group_by(ticker) %>% 
    filter(date >= min(date[transaction_type %in% c("buy", "sell")])) %>% 
    fill(transaction_currency, .direction = "down") %>% 
    mutate(quantity = if_else(!is.na(split), 
                              cumsum(replace_na(quantity, 0)) / split - cumsum(replace_na(quantity, 0)),
                              quantity),
           quantity_cum = round(cumsum(replace_na(quantity, 0)), 4L)) %>% 
    ungroup() %>% 
    filter(quantity_cum != 0 | transaction_type %in% c("buy", "sell"))
}

join_stock_prices <- function (df) {
  df %>% 
    left_join(stock_and_fund_prices, by = c("date", "ticker")) %>% 
    group_by(ticker) %>% # fill closing price with previous value(s)
    fill(closing_price:turnover, .direction = "down") %>% 
    ungroup()
}

join_currencies <- function (df) {
  df %>% 
    left_join(currencies, by = c("date", 
                                 "transaction_currency" = "currency")) %>% 
    mutate(market_exchange_rate = if_else(transaction_currency == "EUR",
                                          1, market_exchange_rate)) %>% 
    group_by(transaction_currency) %>% 
    fill(market_exchange_rate, .direction = "down") %>% 
    ungroup()
}


df_raw <- initialise_dates() %>% 
  join_transactions() %>%
  join_stock_prices() %>% 
  join_currencies() %>% 
  mutate(dividend_eur = replace_na(dividend * quantity_cum * market_exchange_rate, 0))

write_rds(df_raw, str_c(in_dir, "data/processed_portfolio_data.rds"))

today <- today() %>% 
  enframe()

write_rds(today, str_c(in_dir, "data/data_proc_today.rds"))


