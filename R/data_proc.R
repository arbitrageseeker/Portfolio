library(tidyverse)
library(lubridate)
library(aws.s3)

# sources ####

transactions <- s3read_using(FUN = read_rds, bucket = Sys.getenv("bucket"),
                             object = "transactions.rds") 

min_date <- transactions %>% 
  filter(transaction_date == min(transaction_date)) %>% 
  pluck("transaction_date")

ticker_min_dates <- transactions %>% 
  group_by(ticker) %>% 
  summarise(min_date = min(transaction_date)) %>% 
  ungroup()

splits <- s3read_using(FUN = read_rds, bucket = Sys.getenv("bucket"),
                       object = "splits.rds")

dividends <- s3read_using(FUN = read_rds, bucket = Sys.getenv("bucket"),
                          object = "dividends.rds")

transactions_total <- transactions %>% 
  full_join(dividends, by = c("transaction_date", "ticker")) %>% 
  full_join(splits, by = c("transaction_date", "ticker")) %>% 
  left_join(ticker_min_dates, by = "ticker") %>% 
  filter((!is.na(dividend) & transaction_date > min_date) |
           (!is.na(split) & transaction_date >= min_date) |
           !is.na(transaction_type)) %>% 
  mutate(dividend = dividend * replace_na(split, 1))

stock_and_fund_prices <- s3read_using(FUN = read_rds, bucket = Sys.getenv("bucket"),
                                      object = "stock_and_fund_prices.rds")

currencies <- s3read_using(FUN = read_rds, bucket = Sys.getenv("bucket"),
                           object = "currencies.rds")

# processing data from different sources ####

initialise_dates <- function () {
  tibble(date = seq(from = min_date, to = today(), by = "day")) %>% 
    crossing(distinct(transactions_total, ticker))
}

join_transactions <- function (df) {
  df %>%
    left_join(transactions_total, by = c("date" = "transaction_date", "ticker")) %>% 
    group_by(ticker) %>% 
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

write_rds(df_raw, file.path(tempdir(), "processed_portfolio_data.rds"))

put_object(
  file = file.path(tempdir(), "processed_portfolio_data.rds"), 
  object = "processed_portfolio_data.rds", 
  bucket = Sys.getenv("bucket"),
  multipart = T
)
