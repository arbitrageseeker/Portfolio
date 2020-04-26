library(rmarkdown)
library(tidyverse)

source("r/selenium_scrape.R", encoding = "UTF-8")
source("r/transactions.R", encoding = "UTF-8")
source("r/dividends_and_splits.R", encoding = "UTF-8")
source("r/daily_prices.R", encoding = "UTF-8")
source("r/data_proc.R", encoding = "UTF-8")

render("r/markdown.Rmd", "pdf_document", 
       knit_root_dir = getwd(),
       output_file = str_c("C:/Users/anssi/OneDrive/Tiedostot/Portfolio/markdown_reports/portfolio_report_", 
                           Sys.Date(), ".pdf"), 
       clean = TRUE)
