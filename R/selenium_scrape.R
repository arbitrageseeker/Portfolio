library(RSelenium)
library(XML)
library(fs)
library(tidyverse)
library(lubridate)

in_dir <- readLines("in_dir.txt")
selenium_ticker_url <- readLines("selenium_ticker_url.txt")
selenium_ticker_name <- readLines("selenium_ticker_name.txt")

double_clean <- function (vector) {
    str_replace_all(vector, ",", ".") %>% 
    parse_double()
}

eCaps <- list(chromeOptions = 
                list(prefs = list(
                  "profile.default_content_settings.popups" = 0L,
                  "download.prompt_for_download" = FALSE,
                  "download.default_directory" = "/home/seluser/Downloads",
                  "download.directory_upgrade" = TRUE,
                  "safebrowsing.enabled" =TRUE)))

remote_function <- function(source) {
  if (source == "docker") {
    remoteDriver(
      remoteServerAddr = "selenium",
      port = 4444L,
      browserName = "chrome",
      extraCapabilities = eCaps)
      
  } else if (source == "local") {
    remoteDriver(
      remoteServerAddr = "localhost",
      port = 4445L,
      browserName = "chrome",
      extraCapabilities = eCaps)
  }
}

local_wd <- readLines("local_wd.txt")

remDr <- if (getwd() == local_wd) {
  remote_function(source = "local")
} else {
  remote_function(source = "docker")
}

remDr$open()

remDr$navigate(selenium_ticker_url)

Sys.sleep(2)

from_date <- remDr$findElement("id", "FromDate")

from_date$clickElement()

Sys.sleep(1.5)

from_date$clearElement()

from_date$sendKeysToElement(list('2017-01-01', key = "enter"))

Sys.sleep(1.5)

from_date$buttondown()

remDr$executeScript("window.scrollTo(0,700);")

search_button <- remDr$findElement(
  using = "xpath",
  "//*[(@id = 'searchHistoricalFundsId')]//*[contains(concat( ' ', @class, ' ' ), concat( ' ', 'ui-button-text', ' ' ))]")

search_button$sendKeysToElement(list(key = "enter"))

Sys.sleep(1.5)

search_button$clickElement()

Sys.sleep(1.5)

export_excel <- remDr$findElement(using = "id", "exportExcel")

export_excel$clickElement()

Sys.sleep(2)

remDr$quit()

files <- dir_ls("data") %>% 
  enframe() %>% 
  select(value) %>% 
  filter(value == str_extract(value, "data/_SSE.*")) %>% 
  mutate(extract = str_extract(value, "(?<=\\d\\_).*(?=\\.csv)"),
         order_number = if_else(is.na(str_extract(extract, "(?<=[(]).*(?=[)])")) == T,
                                1L,
                                str_extract(extract, "(?<=[(]).*(?=[)])") %>% parse_integer() + 1L),
         helper = str_extract(extract, ".*(?=\\s[(])"),
         date = coalesce(helper, extract) %>% parse_date("%Y-%m-%d")) %>% 
  select(-helper) %>% 
  mutate(remove = case_when(date == max(date) & order_number == max(order_number) ~ FALSE,
                            TRUE ~ TRUE))

old_file_string <- files %>% 
  filter(date == min(date)) %>% 
  filter(order_number == min(order_number)) 

old_file_raw <- read_lines(old_file_string$value,
                  skip = 1) %>% 
  str_remove(";$")
  
old_file <- read_delim(old_file_raw,
                  delim = ";",
                  locale = locale(decimal_mark = ",",
                                  encoding = "UTF-8"),
                  col_types = cols(.default = "c")) %>% 
  transmute(date = ymd(.[[1]]),
            ticker = str_c(selenium_ticker_name, ".HE"),
            closing_price = double_clean(.[[4]]),
            opening_price = lag(closing_price, n = 1),
            lowest_price = double_clean(.[[3]]),
            highest_price = double_clean(.[[2]]),
            closing_adjusted_price = closing_price,
            opening_adjusted_price = opening_price,
            lowest_adjusted_price = lowest_price,
            highest_adjusted_price = highest_price,
            adjusted_dividends_price = closing_price,
            trading_volume = str_remove_all(.[[6]], ",") %>% parse_integer(),
            trading_volume_adjusted = trading_volume) %>% 
  filter(!is.na(opening_price)) %>% 
  arrange(date)

new_file_string <- files %>% 
  filter(date == max(date)) %>% 
  filter(order_number == max(order_number))

new_file_raw <- read_lines(new_file_string$value,
                           skip = 1) %>% 
  str_remove(";$")

new_file <- read_delim(new_file_raw,
                       delim = ";",
                       locale = locale(decimal_mark = ",",
                                       encoding = "UTF-8"),
                       col_types = cols(.default = "c")) %>% 
  transmute(date = ymd(.[[1]]),
            ticker = str_c(selenium_ticker_name, ".HE"),
            closing_price = double_clean(.[[4]]),
            opening_price = lag(closing_price, n = 1),
            lowest_price = double_clean(.[[3]]),
            highest_price = double_clean(.[[2]]),
            closing_adjusted_price = closing_price,
            opening_adjusted_price = opening_price,
            lowest_adjusted_price = lowest_price,
            highest_adjusted_price = highest_price,
            adjusted_dividends_price = closing_price,
            trading_volume = str_remove_all(.[[6]], ",") %>% parse_integer(),
            trading_volume_adjusted = trading_volume) %>% 
  filter(!is.na(opening_price)) %>% 
  arrange(date)

old_rds <- read_rds(str_c("data/", selenium_ticker_name, ".rds"))

new_prices <- new_file %>% 
  anti_join(old_rds, by = "date")

file_to_save <- bind_rows(old_rds, new_prices)

write_rds(file_to_save, str_c(in_dir, "data/", selenium_ticker_name, ".rds"))

files_to_remove <- files %>% 
  filter(remove == TRUE)

walk(files_to_remove$value, file_delete)
