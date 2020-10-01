library(tidyverse)
library(lubridate)
library(readxl)

in_dir <- readLines("in_dir.txt")

# processing transactions data ####

## Nordnet transactions ####

nordnet_transactions_raw <- read_xlsx(str_c(in_dir, "data/transaktionsfil.xlsx"))

double_clean_nordnet <- function (vector) {
  str_replace_all(vector, " ", "") %>%
    str_replace_all(",", ".") %>% 
    parse_double()
}

nordnet_transactions <- nordnet_transactions_raw %>% 
  transmute(financial_institution = "Nordnet",
            transaction_date = ymd(Kirjauspäivä),
            ticker_raw = Arvopaperi,
            transaction_amount_raw = double_clean_nordnet(Summa),
            transaction_type = case_when(
              Tapahtumatyyppi %in% c("PER LUNASTUS OTTO", "DESIM KIRJAUS OTTO", "MYYNTI") == T ~
                "sell",
              Tapahtumatyyppi == "YHTIÖIT. IRR JÄTTÖ" & 
                transaction_amount_raw > 0 ~ "buy",
              Tapahtumatyyppi == "OSTO" ~ "buy"),
            quantity = case_when(
              transaction_type == "sell" ~ 
                (-1L)*double_clean_nordnet(Määrä),
              TRUE ~ double_clean_nordnet(Määrä)),
            transaction_price = double_clean_nordnet(Kurssi),
            transaction_currency = Valuutta,
            transaction_exchange_rate = double_clean_nordnet(Vaihtokurssi),
            transaction_fee_local = double_clean_nordnet(Maksut),
            transaction_fee_eur = transaction_fee_local*transaction_exchange_rate,
            transaction_amount_local = case_when(
              transaction_type %in% c("buy", "sell") ~
                (-1L)*(quantity*transaction_price + transaction_fee_local),
              TRUE ~ quantity*transaction_price - transaction_fee_local),
            transaction_amount_eur = transaction_amount_local*transaction_exchange_rate) %>% 
  filter(transaction_type %in% c("buy", "sell") == T) %>% 
  select(-transaction_amount_raw)

## Nordea transactions ####

double_clean_nordea <- function (vector) {
  str_remove_all(vector, "\\.") %>%
    str_replace_all(",", ".") %>% 
    parse_double()
}

nordea_transactions_raw <- read_xls(str_c(in_dir, "data/Nordea_Transactions.xls"))

nordea_transactions <- nordea_transactions_raw %>% 
  filter(str_to_upper(Tapahtumatyyppi) %in% c("OSTO", "MYYNTI") == T) %>%  
  transmute(financial_institution = "Nordea",
            transaction_date = dmy(Kauppapäivä),
            transaction_type = case_when(str_to_upper(Tapahtumatyyppi) == "OSTO" ~ 
                                           "buy",
                                         str_to_upper(Tapahtumatyyppi) == "MYYNTI" ~
                                           "sell"),
            ticker_raw = Kaupankäyntitunnus,
            quantity = case_when(
              transaction_type == "sell" ~
                double_clean_nordea(Määrä)*(-1L),
              TRUE ~ double_clean_nordea(Määrä)),
            transaction_price = double_clean_nordea(Kurssi),
            transaction_currency = Valuutta...11,
            transaction_exchange_rate = double_clean_nordea(Valuuttakurssi),
            transaction_fee_eur = double_clean_nordea(Palkkio) %>% 
              replace_na(0L),
            transaction_fee_local = transaction_fee_eur/transaction_exchange_rate,
            transaction_amount_raw = case_when(
              transaction_type == "buy" ~ 
                (-1L)*`Transaction amount(Settled)(label.transactiontotalforeign)` %>% 
                double_clean_nordea(),
              TRUE ~ `Transaction amount(Settled)(label.transactiontotalforeign)` %>% 
                double_clean_nordea()),
            transaction_amount_local =  case_when(
              transaction_type %in% c("buy", "sell") ~
                (-1L)*(quantity*transaction_price + transaction_fee_local),
              TRUE ~ quantity*transaction_price - transaction_fee_local),
            transaction_amount_eur = transaction_amount_local*transaction_exchange_rate) %>% 
  select(-transaction_amount_raw)

## Seligson transactions ####

seligson_transactions_raw <- read_xlsx(str_c(in_dir, "data/Seligson_tapahtumat.xlsx"))

seligson_transactions <- seligson_transactions_raw %>% 
  transmute(financial_institution = "Seligson",
            transaction_date = ymd(kauppapvm),
            ticker_raw = rahasto,
            quantity = maara,
            transaction_type = case_when(tapahtuma_arvo < 0 ~ "buy",
                                        tapahtuma_arvo > 0 ~ "sell"),
            transaction_price = abs(tapahtuma_arvo)/quantity,
            transaction_currency = "EUR",
            transaction_exchange_rate = 1L,
            transaction_amount_local = tapahtuma_arvo,
            transaction_amount_eur = tapahtuma_arvo)

# combine ####

transactions_raw <- nordnet_transactions %>% 
  bind_rows(nordea_transactions) %>% 
  bind_rows(seligson_transactions)

map_tickers <- read_xlsx(str_c(in_dir, "data/Map_tickers.xlsx"))

transactions <- transactions_raw %>%
  mutate(ticker = case_when(financial_institution != "Seligson" & transaction_currency == "EUR" ~
                              str_c(ticker_raw, ".HE"),
                            financial_institution != "Seligson" & transaction_currency == "GBX" ~
                              str_c(ticker_raw, ".LSE"),
                            TRUE ~ ticker_raw)) %>% 
  left_join(map_tickers, by = c("ticker_raw" = "orig")) %>% 
  mutate(ticker = if_else(is.na(correct)==T, ticker, correct)) %>% 
  select(-correct, -ticker_raw) %>% 
  group_by(transaction_date, ticker) %>% 
  summarise(financial_institution = last(financial_institution),
            transaction_type = last(transaction_type),
            transaction_price = weighted.mean(transaction_price, quantity),
            quantity = sum(quantity),
            transaction_currency = last(transaction_currency),
            transaction_exchange_rate = last(transaction_exchange_rate),
            transaction_fee_local = sum(transaction_fee_local),
            transaction_fee_eur = sum(transaction_fee_eur),
            transaction_amount_local = sum(transaction_amount_local),
            transaction_amount_eur = sum(transaction_amount_eur)) %>% 
  ungroup()

transactions_old <- read_rds(str_c(in_dir, "data/transactions.rds"))

transactions_new <- transactions %>% 
  anti_join(transactions_old, by = c("financial_institution", "transaction_date", "transaction_type", 
                                     "quantity", "ticker"))

transactions_to_save <- bind_rows(transactions_old, transactions_new)

write_rds(transactions_to_save, str_c(in_dir, "data/transactions.rds"))
