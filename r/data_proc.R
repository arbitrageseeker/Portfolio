library(tidyverse)
library(lubridate)

in_dir <- readLines("in_dir.txt")

# sources ####

transactions <- read_rds(str_c(in_dir, "data/transactions.rds")) 

min_date <- transactions %>% 
  filter(transaction_date == min(transaction_date)) %>% 
  pluck("transaction_date")

ticker_min_dates <- transactions %>% 
  group_by(ticker) %>% 
  summarise(min_date = min(transaction_date)) %>% 
  ungroup()

splits <- read_rds(str_c(in_dir, "data/splits.rds"))

dividends <- read_rds(str_c(in_dir, "data/dividends.rds"))

transactions_total <- transactions %>% 
  full_join(dividends, by = c("transaction_date", "ticker")) %>% 
  full_join(splits, by = c("transaction_date", "ticker")) %>% 
  left_join(ticker_min_dates, by = "ticker") %>% 
  filter((!is.na(dividend) & transaction_date > min_date) |
           (!is.na(split) & transaction_date >= min_date) |
           !is.na(transaction_type)) %>% 
  mutate(dividend = dividend * replace_na(split, 1))

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

# portfolio account balance ####

df_account_raw <- df_raw %>% 
  group_by(date) %>% 
  summarise(account_flow = sum(replace_na(transaction_amount_eur, 0) + replace_na(dividend_eur, 0)),
            transaction_flow = sum(transaction_amount_eur, na.rm = T),
            dividend_flow = sum(dividend_eur, na.rm = T)) %>% 
  ungroup() %>% 
  arrange(date) %>% 
  mutate(dividend_cum = cumsum(dividend_flow))


balance <- if (df_account_raw$account_flow[1] < 0) {
  0
} else {
  df_account_raw$account_flow[1]
}

for (i in 2:nrow(df_account_raw)) {
  
  balance[i] <- if (df_account_raw$account_flow[i] + balance[i-1] < 0) {
    0
  } else {
    df_account_raw$account_flow[i] + balance[i-1]
  }
  
}

balances <- balance %>% 
  enframe() %>% 
  select(balance = value)

df_account_raw2 <- df_account_raw %>% 
  bind_cols(balances) %>% 
  mutate(transaction_flow_adj = transaction_flow + replace_na(lag(balance), 0)) %>% 
  filter(transaction_flow_adj < 0) %>% 
  select(date, transaction_flow_adj)

df_account <- df_account_raw %>% 
  bind_cols(balances) %>% 
  select(date, balance) %>% 
  left_join(df_account_raw2, by = "date") %>% 
  mutate(transaction_flow_adj = replace_na(transaction_flow_adj, 0))

write_rds(df_account, str_c(in_dir, "data/processed_account_balance.rds"))
