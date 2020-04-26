library(tidyverse)
library(lubridate)
library(readxl)
library(tidyquant)

in_dir <- readLines("in_dir.txt")
quandl_api_key(readLines("quandl_api_key.txt"))
selenium_ticker_name <- readLines("selenium_ticker_name.txt")
merged_ticker_name <- readLines("merged_ticker_name.txt")

# tickers from transactions data ####

transactions <- read_rds(str_c(in_dir, "data/transactions.rds")) 

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
                 from = "1970-01-01",
                 to = Sys.Date()) %>% 
  transmute(transaction_type = "split",
            transaction_date = date,
            ticker = symbol,
            split = splits)

# Nordnet dividends ####

nordnet_transactions_raw <- read_delim(file = str_c(in_dir, "data/transaktionsfil.csv"),
                                       delim = ";",
                                       trim_ws = T,
                                       locale = locale(encoding = "ISO-8859-1"),
                                       col_types = cols(.default = "c"))

double_clean_nordnet <- function (vector) {
  str_replace_all(vector, " ", "") %>%
    str_replace_all(",", ".") %>% 
    parse_double()
}

nordnet_dividends <- nordnet_transactions_raw %>% 
  filter(Tapahtumatyyppi %in% c("OSINKO", "OSINGON PERUUTUS")) %>% 
  transmute(financial_institution = "Nordnet",
            transaction_type = "dividend",
            transaction_date = ymd(Kirjauspäivä),
            ticker_raw = Arvopaperi,
            dividend_ex = double_clean_nordnet(Summa),
            transaction_currency = Valuutta,
            exchange_rate = double_clean_nordnet(Valuuttakurssi),
            dividend_eur = dividend_ex*exchange_rate)

## Nordea dividends ####

nordea_dividends_raw <- read_xls(str_c(in_dir, "data/YieldAndDividend.xls"))

nordea_names_and_tickers <- read_xls(str_c(in_dir, "data/Nordea_Transactions.xls")) %>% 
  filter(!is.na(Kaupankäyntitunnus)) %>% 
  distinct(ticker_name_raw = Nimi, ticker_raw = Kaupankäyntitunnus,
           transaction_currency = Valuutta...11)

nordea_dividends <- nordea_dividends_raw %>% 
  filter(Tapahtumalaji == "Osinko") %>% 
  transmute(financial_institution = "Nordea",
            transaction_type = "dividend",
            transaction_date = as.Date(Tap.pvm, format = "%d.%m.%Y"),
            ticker_name_raw = Nimi,
            dividend_eur = Brutto) %>% 
  left_join(nordea_names_and_tickers, by = "ticker_name_raw") %>% 
  select(-ticker_name_raw)

# dividends_merged_ticker <- tq_get(str_c("WIKI/", merged_ticker_name),
#                              get = "quandl",
#                              from = min_date,
#                              to = Sys.Date()) %>%
#   transmute(transaction_type = "dividend",
#             transaction_date = date,
#             ticker = merged_ticker_name,
#             dividend_ex = ex.dividend) %>% 
#   filter(dividend_ex > 0)

# dividends <- tq_get(tickers_vec,
#                     get = "dividends",
#                     from = min_date,
#                     to = Sys.Date()) %>% 
#   transmute(transaction_type = "dividend",
#             transaction_date = date,
#             ticker = symbol,
#             dividend_ex = dividends) %>% 
#   bind_rows(dividends_merged_ticker)

# combine ####

dividends_raw <- nordnet_dividends %>% 
  bind_rows(nordea_dividends)

map_tickers <- read_xlsx(str_c(in_dir, "data/Map_tickers.xlsx"))

dividends <- dividends_raw %>%
  mutate(ticker = case_when(transaction_currency == "EUR" ~
                              str_c(ticker_raw, ".HE"),
                            transaction_currency == "GBX" ~
                              str_c(ticker_raw, ".L"),
                            TRUE ~ ticker_raw)) %>% 
  left_join(map_tickers, by = c("ticker_raw" = "orig")) %>% 
  mutate(ticker = if_else(is.na(correct)==T, ticker, correct)) %>% 
  select(-correct, -ticker_raw) %>% 
  group_by(ticker, transaction_date, transaction_type, transaction_currency) %>% 
  summarise(dividend_eur = sum(dividend_eur, na.rm = T)) %>% 
  ungroup()

# save ####

## splits ####

splits_old <- read_rds(str_c(in_dir, "data/splits.rds"))

splits_new <- splits %>% 
  anti_join(splits_old, by = c("transaction_type", "transaction_date", "ticker"))

splits_to_save <- bind_rows(splits_old, splits_new)

write_rds(splits_to_save, str_c(in_dir, "data/splits.rds"))

## dividends ####

dividends_old <- read_rds(str_c(in_dir, "data/dividends.rds"))

dividends_new <- dividends %>% 
  anti_join(dividends_old, by = c("transaction_type", "transaction_date", "ticker"))

dividends_to_save <- bind_rows(dividends_old, dividends_new)

write_rds(dividends_to_save, str_c(in_dir, "data/dividends.rds"))
