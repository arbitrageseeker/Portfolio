library(tidyverse)
library(lubridate)
library(readxl)
library(aws.s3)

# processing transactions data ####

## Nordnet transactions ####

nordnet_transactions_raw <- s3read_using(FUN = read_xlsx, bucket = Sys.getenv("bucket"),
                                         object = "transaktionsfil.xlsx")

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
    str_replace_all(vector, ",", ".") %>% 
    parse_double()
}

nordea_transactions_raw <- s3read_using(FUN = read_xlsx, bucket = Sys.getenv("bucket"),
                                        object = "Nordea_Transactions.xls")

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

seligson_transactions_raw <- s3read_using(FUN = read_xlsx, bucket = Sys.getenv("bucket"),
                                          object = "Seligson_tapahtumat.xlsx")

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

## Voima transaction ####

read_csv_mod <- function(file) {
  
  read_csv(file,
           col_types = cols(.default = "c"))
}

voima_transactions_raw <- s3read_using(FUN = read_csv_mod, bucket = Sys.getenv("bucket"),
                                       object = "voima_gold_transactions.csv")

currencies <- tibble(date = seq(from = as.Date("2020-01-02"), 
                                to = today(), 
                                by = "day"),
                     currency = "USD") %>% 
  left_join(s3read_using(FUN = read_rds, bucket = Sys.getenv("bucket"),
                          object = "currencies.rds"), by = c("date", "currency")) %>% 
  arrange(date) %>% 
  fill(market_exchange_rate, .direction = "down") %>% 
  ungroup()

voima_transactions_raw2 <- voima_transactions_raw %>% 
  transmute(financial_institution = "Voima Gold",
            transaction_date = ymd_hms(Date) %>% as_date(),
            ticker_raw = "GC.COMM",
            transaction_type_raw = Type,
            transaction_type = case_when(Type == "purchase" ~ "buy",
                                         Type == "storage_fee" ~ "sell",
                                         Type == "sell" ~ "sell"),
            quantity_raw = parse_double(`Gold change total (grams)`),
            quantity = quantity_raw / 31.1034768,
            transaction_currency = "USD",
            transaction_exchange_rate = 1,
            transaction_fee_local = abs(parse_double(`Fee euro`)),
            transaction_fee_eur = transaction_fee_local,
            transaction_amount_eur = parse_double(`Currency change total (€)`),
            transaction_price = parse_double(`Market price`) * 31.1034768) %>% 
  left_join(currencies, by = c("transaction_currency" = "currency", 
                               "transaction_date" = "date")) %>% 
  mutate(transaction_price = transaction_price / market_exchange_rate,
         transaction_fee_local = transaction_fee_local / market_exchange_rate,
         transaction_amount_local = case_when(
           transaction_type_raw == "storage_fee" ~ 0,
           transaction_type %in% c("buy", "sell") ~
             (-1L)*(quantity*transaction_price + transaction_fee_local),
           TRUE ~ quantity*transaction_price - transaction_fee_local))

monthly_fees_raw <- tibble(date = seq(from = min(voima_transactions_raw2$transaction_date), 
                                  to = make_date(year(today()), month(today()), 1), by = "day")) %>% 
  left_join(voima_transactions_raw2 %>% 
              filter(transaction_type_raw != "storage_fee") %>% 
              select(transaction_date, quantity_raw, transaction_amount_eur,
                     transaction_fee_eur),
            by = c("date" = "transaction_date")) %>% 
  mutate(quantity_cum_raw = cumsum(coalesce(quantity_raw, 0)),
         quantity_cum = accumulate(coalesce(quantity_raw, 0), ~ .y + .x * (1 - 0.0099 / 365.25)),
         fee_daily = quantity_cum - lag(quantity_cum) - coalesce(quantity_raw, 0),
         month = make_date(year(date), month(date), 1)) %>%
  group_by(month) %>% 
  mutate(fee_monthly = sum(coalesce(fee_daily,  0)) %>% 
           round(digits = 3L)) %>% 
  ungroup() %>%
  mutate(fee_monthly_lag = lag(fee_monthly)) %>% 
  filter(date == month)

commodities <- s3read_using(FUN = read_rds, bucket = Sys.getenv("bucket"),
                            object = "commodities.rds") %>% 
  filter(ticker == "GC.COMM") %>% 
  select(date, ticker, closing_price)

monthly_fees <- monthly_fees_raw %>% 
  mutate(ticker = "GC.COMM") %>% 
  left_join(commodities, by = c("date", "ticker")) %>% 
  left_join(currencies, by = "date") %>% 
  fill(closing_price, .direction = "down") %>% 
  transmute(financial_institution = "Voima Gold",
            transaction_date = date,
            ticker_raw = ticker,
            transaction_type_raw = "storage_fee",
            transaction_type = "sell",
            quantity_raw = fee_monthly_lag,
            quantity = quantity_raw / 31.1034768,
            transaction_currency = "USD",
            transaction_exchange_rate = 1,
            transaction_fee_local = quantity * closing_price,
            transaction_fee_eur = transaction_fee_local * market_exchange_rate,
            transaction_amount_eur = 0,
            transaction_amount_local = 0,
            transaction_price = 0)

voima_transactions <- voima_transactions_raw2 %>% 
  filter(transaction_type_raw != "storage_fee") %>% 
  select(-market_exchange_rate) %>% 
  bind_rows(monthly_fees)
  

# combine ####

transactions_raw <- nordnet_transactions %>% 
  bind_rows(nordea_transactions) %>% 
  bind_rows(seligson_transactions) %>% 
  bind_rows(voima_transactions)

map_tickers <- s3read_using(FUN = read_xlsx, bucket = Sys.getenv("bucket"),
                            object = "Map_tickers.xlsx")

transactions <- transactions_raw %>%
  mutate(ticker = case_when(!financial_institution %in% c("Seligson", "Voima Gold") & transaction_currency == "EUR" ~
                              str_c(ticker_raw, ".HE"),
                            !financial_institution %in% c("Seligson", "Voima Gold") & transaction_currency == "GBX" ~
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

transactions_old <- s3read_using(FUN = read_rds, bucket = Sys.getenv("bucket"),
                                 object = "transactions.rds")

transactions_new <- transactions %>% 
  anti_join(transactions_old, by = c("financial_institution", "transaction_date", "transaction_type", "ticker"))

transactions_to_save <- bind_rows(transactions_old, transactions_new)

write_rds(transactions_to_save, file.path(tempdir(), "transactions.rds"))

put_object(
  file = file.path(tempdir(), "transactions.rds"), 
  object = "transactions.rds", 
  bucket = Sys.getenv("bucket")
)
