library(RSelenium)
library(XML)
library(fs)
library(tidyverse)
library(lubridate)

in_dir <- readLines("in_dir.txt")
selenium_ticker_url <- readLines("selenium_ticker_url.txt")
selenium_ticker_name <- readLines("selenium_ticker_name.txt")

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
  filter(date == min(date) & order_number == min(order_number)) 

old_file_raw <- read_lines(old_file_string$value,
                  skip = 1) %>% 
  str_remove(";$")
  
old_file <- read_delim(old_file_raw,
                  delim = ";",
                  locale = locale(decimal_mark = ",",
                                  encoding = "UTF-8"),
                  col_types = cols(.default = "c")) %>% 
  transmute(date = ymd(Date),
            ticker = str_c(selenium_ticker_name, ".HE"),
            closing_price = parse_double(str_replace_all(`Closing price`, "\\,", "\\."))) %>% 
  filter(date >= "2013-06-07") %>% 
  arrange(date)

new_file_string <- files %>% 
  filter(date == max(date) & order_number == max(order_number))

new_file_raw <- read_lines(new_file_string$value,
                           skip = 1) %>% 
  str_remove(";$")

new_file <- read_delim(new_file_raw,
                       delim = ";",
                       locale = locale(decimal_mark = ",",
                                       encoding = "UTF-8"),
                       col_types = cols(.default = "c")) %>% 
  transmute(date = ymd(Date),
            ticker = str_c(selenium_ticker_name, ".HE"),
            closing_price = parse_double(str_replace_all(`Closing price`, "\\,", "\\."))) %>% 
  filter(date > max(old_file$date)) %>% 
  arrange(date)

file_to_save <- bind_rows(old_file, new_file)

write_rds(file_to_save, str_c(in_dir, "/", selenium_ticker_name, ".rds"))

files_to_remove <- files %>% 
  filter(remove == TRUE)

walk(files_to_remove$value, file_delete)
