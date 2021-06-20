library(tidyverse)
library(lubridate)
library(shiny)
library(shinydashboard)
library(scales)
library(shinythemes)
library(shinyWidgets)
library(glue)
library(leaflet)
library(aws.s3)
library(treemap)
library(d3treeR)
library(highcharter)
library(ggthemes)

pdf(NULL)

df_raw <- s3read_using(FUN = read_rds, bucket = Sys.getenv("bucket"), object = "processed_portfolio_data.rds")

df_fundamentals <- s3read_using(FUN = read_rds, bucket = Sys.getenv("bucket"), object = "fundamentals_general.rds")

transactions <- s3read_using(FUN = read_rds, bucket = Sys.getenv("bucket"), object = "transactions.rds")

df_fundamentals_financials <- s3read_using(FUN = read_rds, bucket = Sys.getenv("bucket"), 
                                           object = "fundamentals_current_financials.rds")

seligson <- transactions %>% 
  filter(financial_institution == "Seligson") %>% 
  distinct(ticker) %>% 
  pluck("ticker")

bte <- transactions %>% 
  filter(ticker == "BITCOIN XBTE.HE") %>% 
  distinct(ticker) %>% 
  pluck("ticker")

df_fundamentals2 <- df_raw %>% 
  distinct(ticker) %>% 
  left_join(df_fundamentals, by = "ticker") %>% 
  mutate(name_adj = coalesce(name, ticker),
         industry_gic = case_when(ticker %in% seligson ~ "Rahastot",
                                  ticker %in% bte ~ "ETF",
                                  TRUE ~ coalesce(industry_gic, industry, "Muut")),
         sector_gic = case_when(ticker %in% seligson ~ "Rahastot",
                                ticker %in% bte ~ "ETF",
                                TRUE ~ coalesce(sector_gic, sector, "Muut")))

df <- df_raw %>% 
  left_join(df_fundamentals2, by = "ticker") %>% 
  mutate(industry_gic = case_when(ticker %in% seligson ~ "Rahastot",
                                  ticker %in% bte ~ "ETF",
                                  TRUE ~ coalesce(industry_gic, industry, "Muut")),
         sector_gic = case_when(ticker %in% seligson ~ "Rahastot",
                                ticker %in% bte ~ "ETF",
                                TRUE ~ coalesce(sector_gic, sector, "Muut")),
         country = case_when(ticker %in% seligson ~ "Rahastot",
                             ticker %in% bte ~ "ETF",
                             TRUE ~ coalesce(country, "Muut")))

default_stocks <- df %>% 
  mutate(price_eur = closing_adjusted_price*market_exchange_rate) %>%
  filter(date >= make_date(year(today()), 1, 1)) %>% 
  group_by(name_adj) %>% 
  mutate(return = round(100*(price_eur / first(price_eur) - 1), 2L)) %>% 
  ungroup() %>% 
  filter(date == max(date)) %>% 
  select(name_adj, return) %>% 
  mutate(rank = rank(return)) %>% 
  filter(rank <= 3 | rank >= max(rank) - 2) %>% 
  pluck("name_adj")

min_date <- min(df$date)

indices <- s3read_using(FUN = read_rds, bucket = Sys.getenv("bucket"), object = "indices.rds")

commodities <- s3read_using(FUN = read_rds, bucket = Sys.getenv("bucket"), object = "commodities.rds")

sidebar <- sidebarPanel(dateRangeInput("date", "Päivämäärä", 
                                       start = today() %m-% months(3),
                                       end = max(df$date, na.rm = T),
                                       min = min(df$date, na.rm = T),
                                       max = max(df$date, na.rm = T),
                                       weekstart = 1L,
                                       language = "fi",
                                       separator = " - "),
                        awesomeRadio("osingot_laskenta", "Osinkojen laskenta",
                                     choices = c("Ei osinkoja", "Osingot lisättynä pääomaan",
                                                 "Osingot uudelleen sijoitettuna"),
                                     selected = "Osingot uudelleen sijoitettuna",
                                     inline = T),
                        awesomeCheckboxGroup("financial_institution", "Rahoituslaitos",
                                             inline = T,
                                             choices = df$financial_institution[!is.na(df$financial_institution)] %>% 
                                               unique() %>% sort()),
                        awesomeCheckboxGroup("currency", "Arvopaperin valuutta",
                                             inline = T,
                                             choices = df$transaction_currency %>%
                                               unique() %>% sort()),
                        awesomeCheckboxGroup("country", "Arvopaperin maa",
                                             inline = T,
                                             choices = df_fundamentals2$country %>%
                                               unique() %>% sort()),
                        awesomeCheckboxGroup("sector", "Arvopaperin toimialasektori",
                                             choices = df_fundamentals2$sector_gic %>%
                                               unique() %>% sort(),
                                             inline = T),
                        selectizeInput("industry", "Arvopaperin toimiala",
                                       choices = df_fundamentals2$industry_gic %>%
                                         unique() %>% sort(),
                                       multiple = T),
                        selectizeInput("security", "Arvopaperi",
                                       choices = df_fundamentals2$name_adj %>%
                                         unique() %>% sort(),
                                       multiple = T)
)

sidebar2 <- sidebarPanel(dateRangeInput("date_macro", "Päivämäärä", 
                                        start = make_date(year(today()), 1, 1),
                                        end = max(indices$date, na.rm = T),
                                        min = min(indices$date, na.rm = T),
                                        max = max(indices$date, na.rm = T),
                                        weekstart = 1L,
                                        language = "fi",
                                        separator = " - "),
                         awesomeCheckboxGroup("index_macro", "Indeksi",
                                              choices = indices$index %>%
                                                unique() %>% sort(),
                                              selected = "S&P 500",
                                              inline = T),
                         selectizeInput("commodity", "Raaka-aine",
                                        choices = commodities$commodity %>%
                                          unique() %>% sort(),
                                        selected = "Gold",
                                        multiple = T),
                         awesomeRadio("annualisoitu", "Tavallinen/Annualisoitu",
                                      choices = c("Tavallinen", "Annualisoitu"),
                                      selected = "Tavallinen",
                                      inline = T)
)

ui <- navbarPage(
  title = "Osake- ja rahastosalkku",
  theme = shinytheme("superhero"),
  tabPanel("Päänäkymä",
           tags$head(tags$style(".checkbox-inline {margin: 0 !important;}
                                .radio-inline {margin:  0 !important;}")),
           sidebarLayout(sidebar,
                         mainPanel(
                           fluidRow(valueBoxOutput("portfolio_return"),
                                    valueBoxOutput("portfolio_value"),
                                    valueBoxOutput("largest_return"),
                                    valueBoxOutput("smallest_return"),
                                    valueBoxOutput("purchases"),
                                    valueBoxOutput("sells")),
                           h2("Kehitys"),
                           h3("Tuoton kehitys"),
                           plotOutput("returnplot", height = 600),
                           awesomeCheckboxGroup("index", label = "Indeksi",
                                                choices = indices$index %>%
                                                  unique() %>% sort(),
                                                selected = c("S&P 500", "OMX Helsinki 25"),
                                                inline = T),
                           br(),
                           fluidRow(column(6,
                                           plotOutput("returnplot_countries", height = 600)),
                                    column(6,
                                           plotOutput("returnplot_sectors", height = 600))),
                           br(),
                           br(),
                           fluidRow(column(6, 
                                           highchartOutput("barchart_stock_returns", height = 600)),
                                    column(6, 
                                           highchartOutput("barchart_industry_returns", height = 600))),
                           h4("Tuoton kehitys arvopapereittain"),
                           plotOutput("returnplot_stocks", height = 600),
                           awesomeCheckboxGroup("stock", label = "Arvopaperi",
                                                choices = df_fundamentals2$name_adj %>%
                                                  unique() %>% sort(),
                                                selected = default_stocks,
                                                inline = T),
                           h3("Arvon kehitys"),
                           plotOutput("valueplot", height = 600),
                           h2("Allokaatio"),
                           fluidRow(column(6, 
                                           d3tree2Output("treemap", height = 600)),
                                    column(6, 
                                           d3tree2Output("treemap2", height = 600)),
                                    column(6, 
                                           highchartOutput("allocation_bar1", height = 600)),
                                    column(6, 
                                           highchartOutput("allocation_bar2", height = 600))),
                           h2("Osakkeet maantieteellisesti"),
                           leafletOutput("map", height = 800),
                           br(),
                           h6("Tänään: ", now()),
                         )
           )
  ),
  tabPanel("Makro",
           tags$head(tags$style(".checkbox-inline {margin: 0 !important;}
                                .radio-inline {margin:  0 !important;}")),
           sidebarLayout(sidebar2,
                         mainPanel(
                           h2("Indeksit"),
                           br(),
                           plotOutput("indices_plot"),
                           br(),
                           h2("Raaka-aineet"),
                           br(),
                           plotOutput("commodities_plot"),
                           br(), br()
                         )
           )
  ),
  tabPanel("Arvopaperit",
           tags$head(tags$style(".checkbox-inline {margin: 0 !important;}
                                .radio-inline {margin:  0 !important;}")),
           sidebarLayout(sidebar,
                         mainPanel(
                           h2("Tunnusluvut"),
                           br(),
                           dataTableOutput("tunnusluvut_table"),
                           br(), br()
                         )
           )
  )
)


server <- function(input, output, session) {
  
  observeEvent(session$input$logout,{
    function() {
      stopApp()
    }
  })
  
  observeEvent(session$input$logout2,{
    function() {
      stopApp()
    }
  })
  
  session$onSessionEnded(function() {
    stopApp()
  })
  
  filtered_data <- reactive({
    
    financial_institution_filter <- if (is.null(input$financial_institution))
      df$financial_institution[!is.na(df$financial_institution)] %>% unique()
    else input$financial_institution
    
    security_filter <- if (is.null(input$security))
      df_fundamentals2$name_adj %>% unique() 
    else input$security
    
    securities <- df_fundamentals2 %>% 
      filter(name_adj %in% security_filter) %>% 
      pluck("ticker")
    
    country_filter <- if (is.null(input$country))
      df_fundamentals2$country %>% unique() 
    else input$country
    
    countries <- df_fundamentals2 %>% 
      filter(country %in% country_filter) %>% 
      pluck("ticker")
    
    sector_filter <- if (is.null(input$sector))
      df_fundamentals2$sector_gic %>% unique() 
    else input$sector
    
    sectors <- df_fundamentals2 %>% 
      filter(sector_gic %in% sector_filter) %>% 
      pluck("ticker")
    
    industry_filter <- if (is.null(input$industry_gic))
      df_fundamentals2$industry_gic %>% unique() 
    else input$industry_gic
    
    industries <- df_fundamentals2 %>% 
      filter(industry_gic %in% industry_filter) %>% 
      pluck("ticker")
    
    currency_filter <- if (is.null(input$currency))
      df$transaction_currency %>% unique() 
    else input$currency
    
    filtered <- df %>% 
      filter(!is.na(financial_institution)) %>% 
      filter(financial_institution %in% financial_institution_filter) %>% 
      distinct(ticker, date) %>% 
      group_by(ticker) %>% 
      summarise(min_institution_date = min(date)) %>% 
      ungroup()
    
    filtered2 <- df %>% 
      inner_join(filtered, by = "ticker") %>% 
      filter(date >= min_institution_date) %>% 
      group_by(ticker) %>% 
      mutate(quantity = if_else((is.na(financial_institution) | 
                                   !financial_institution %in% financial_institution_filter) & is.na(split), 
                                0, 
                                quantity),
             quantity_cum = round(cumsum(replace_na(quantity, 0)), 4L),
             transaction_amount_eur = if_else(is.na(financial_institution) | 
                                                !financial_institution %in% financial_institution_filter, 
                                              0, 
                                              transaction_amount_eur)) %>% 
      ungroup() %>% 
      filter(quantity_cum != 0 | transaction_type %in% c("buy", "sell")) %>% 
      mutate(dividend_eur = replace_na(dividend * quantity_cum * market_exchange_rate, 0))
    
    filtered3 <- filtered2 %>%
      filter(date >= input$date[1],
             date <= input$date[2],
             ticker %in% securities,
             ticker %in% countries,
             ticker %in% sectors,
             ticker %in% industries,
             transaction_currency %in% currency_filter)
    
    if (is.null(input$osingot_laskenta) == T | input$osingot_laskenta == "Osingot uudelleen sijoitettuna") {
      
      df_account_raw <- filtered3 %>% 
        group_by(date) %>% 
        summarise(account_flow = sum(replace_na(transaction_amount_eur, 0) + replace_na(dividend_eur, 0)),
                  transaction_flow = sum(transaction_amount_eur, na.rm = T),
                  dividend_flow = sum(dividend_eur, na.rm = T)) %>% 
        ungroup() %>% 
        arrange(date) %>% 
        mutate(dividend_cum = cumsum(dividend_flow),
               transaction_amount_cum = cumsum(transaction_flow))
      
      
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
        select(date, balance,  dividend_cum, transaction_flow, transaction_amount_cum) %>% 
        left_join(df_account_raw2, by = "date") %>% 
        mutate(transaction_flow_adj = replace_na(transaction_flow_adj, 0))
      
      filtered_grouped <- filtered3 %>%
        group_by(date) %>%
        summarise(portfolio_value = sum(quantity_cum * closing_price * market_exchange_rate, na.rm =  T)) %>%
        ungroup() %>%
        left_join(df_account, by = "date") %>% 
        mutate(date_index = date - first(date),
               indeksierotus_twr = case_when(date == min_date ~ portfolio_value / abs(transaction_flow_adj),
                                             date_index == 0 ~ portfolio_value / portfolio_value,
                                             TRUE ~ (portfolio_value + transaction_flow_adj + balance)/
                                               (lag(portfolio_value, n = 1) + lag(balance, n = 1))),
               indeksiluku_twr = 100*cumprod(indeksierotus_twr))
      
      filtered_grouped
      
    } else if (input$osingot_laskenta == "Ei osinkoja") {
      
      df_account_raw <- filtered3 %>% 
        group_by(date) %>% 
        summarise(account_flow = sum(replace_na(transaction_amount_eur, 0)),
                  transaction_flow = sum(transaction_amount_eur, na.rm = T),
                  dividend_flow = sum(dividend_eur, na.rm = T)) %>% 
        ungroup() %>% 
        arrange(date) %>% 
        mutate(dividend_cum = cumsum(dividend_flow),
               transaction_amount_cum = cumsum(transaction_flow))
      
      
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
        select(date, balance,  dividend_cum, transaction_flow, transaction_amount_cum) %>% 
        left_join(df_account_raw2, by = "date") %>% 
        mutate(transaction_flow_adj = replace_na(transaction_flow_adj, 0))
      
      filtered_grouped <- filtered3 %>%
        group_by(date) %>%
        summarise(portfolio_value = sum(quantity_cum * closing_price * market_exchange_rate, na.rm =  T)) %>%
        ungroup() %>%
        left_join(df_account, by = "date") %>% 
        mutate(date_index = date - first(date),
               indeksierotus_twr = case_when(date == min_date ~ portfolio_value / abs(transaction_flow_adj),
                                             date_index == 0 ~ portfolio_value / portfolio_value,
                                             TRUE ~ (portfolio_value + transaction_flow_adj + balance)/
                                               (lag(portfolio_value, n = 1) + lag(balance, n = 1))),
               indeksiluku_twr = 100*cumprod(indeksierotus_twr))
      
      filtered_grouped
      
    } else {
      
      df_account_raw <- filtered3 %>% 
        group_by(date) %>% 
        summarise(account_flow = sum(replace_na(transaction_amount_eur, 0)),
                  transaction_flow = sum(transaction_amount_eur, na.rm = T),
                  dividend_flow = sum(dividend_eur, na.rm = T)) %>% 
        ungroup() %>% 
        arrange(date) %>% 
        mutate(dividend_cum = cumsum(dividend_flow),
               transaction_amount_cum = cumsum(transaction_flow))
      
      
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
        select(date, balance,  dividend_cum, transaction_flow, transaction_amount_cum) %>% 
        left_join(df_account_raw2, by = "date") %>% 
        mutate(transaction_flow_adj = replace_na(transaction_flow_adj, 0))
      
      filtered_grouped <- filtered3 %>%
        group_by(date) %>%
        summarise(portfolio_value = sum(quantity_cum * closing_price * market_exchange_rate, na.rm =  T)) %>%
        ungroup() %>%
        left_join(df_account, by = "date") %>% 
        mutate(date_index = date - first(date),
               indeksierotus_twr = case_when(date == min_date ~ portfolio_value / abs(transaction_flow_adj),
                                             date_index == 0 ~ portfolio_value / portfolio_value,
                                             TRUE ~ (portfolio_value + transaction_flow_adj + balance + dividend_cum)/
                                               (lag(portfolio_value, n = 1) + lag(balance, n = 1) + lag(dividend_cum))),
               indeksiluku_twr = 100*cumprod(indeksierotus_twr))
      
      filtered_grouped
      
    }
    
  })
  
  
  filtered_stock_data <- reactive({
    
    financial_institution_filter <- if (is.null(input$financial_institution))
      df$financial_institution[!is.na(df$financial_institution)] %>% unique()
    else input$financial_institution
    
    security_filter <- if (is.null(input$security))
      df_fundamentals2$name_adj %>% unique()
    else input$security
    
    securities <- df_fundamentals2 %>% 
      filter(name_adj %in% security_filter) %>% 
      pluck("ticker")
    
    country_filter <- if (is.null(input$country))
      df_fundamentals2$country %>% unique() 
    else input$country
    
    countries <- df_fundamentals2 %>% 
      filter(country %in% country_filter) %>% 
      pluck("ticker")
    
    sector_filter <- if (is.null(input$sector))
      df_fundamentals2$sector_gic %>% unique() 
    else input$sector
    
    sectors <- df_fundamentals2 %>% 
      filter(sector_gic %in% sector_filter) %>% 
      pluck("ticker")
    
    industry_filter <- if (is.null(input$industry_gic))
      df_fundamentals2$industry_gic %>% unique() 
    else input$industry_gic
    
    industries <- df_fundamentals2 %>% 
      filter(industry_gic %in% industry_filter) %>% 
      pluck("ticker")
    
    currency_filter <- if (is.null(input$currency))
      df$transaction_currency %>% unique() 
    else input$currency
    
    filtered <- df %>% 
      filter(!is.na(financial_institution)) %>% 
      filter(financial_institution %in% financial_institution_filter) %>% 
      distinct(ticker, date) %>% 
      group_by(ticker) %>% 
      summarise(min_institution_date = min(date)) %>% 
      ungroup()
    
    filtered2 <- df %>% 
      inner_join(filtered, by = "ticker") %>% 
      filter(date >= min_institution_date) %>% 
      group_by(ticker) %>% 
      mutate(quantity = if_else((is.na(financial_institution) | 
                                   !financial_institution %in% financial_institution_filter) & is.na(split),  
                                0, 
                                quantity),
             quantity_cum = round(cumsum(replace_na(quantity, 0)), 4L)) %>% 
      ungroup() %>% 
      filter(quantity_cum != 0 | transaction_type %in% c("buy", "sell"))
    
    filtered3 <- filtered2 %>%
      filter(date >= input$date[1],
             date <= input$date[2],
             ticker %in% securities,
             ticker %in% countries,
             ticker %in% sectors,
             ticker %in% industries,
             transaction_currency %in% currency_filter) %>% 
      group_by(ticker) %>% 
      mutate(indeksiluku_twr = 100 * (adjusted_dividends_price/
                                        first(adjusted_dividends_price)),
             indeksierotus_twr = adjusted_dividends_price/
                                        lag(adjusted_dividends_price)) %>% 
      ungroup()
    
    filtered3
    
  })
  
  indices_date <- reactive({
    
    indices_filtered <- indices %>%
      filter(date >= input$date[1],
             date <= input$date[2]) %>% 
      group_by(ticker) %>% 
      mutate(indeksiluku_twr = 100 * (adjusted_dividends_price/
                                        first(adjusted_dividends_price))) %>% 
      ungroup()
    
    indices_filtered
    
  })
  
  indices_date2 <- reactive({
    
    indices_filtered <- indices %>%
      filter(date >= input$date_macro[1],
             date <= input$date_macro[2]) %>% 
      group_by(ticker) %>% 
      mutate(date_index = date - first(date),
             indeksiluku_twr = 100 * (adjusted_dividends_price/
                                        first(adjusted_dividends_price)),
             twr_annualised = if_else(date_index < 250, NA_real_,
                                      100 * ((indeksiluku_twr/100)^(365.25/(as.integer(date_index) + 1)) - 1))) %>% 
      ungroup()
    
    indices_filtered
    
  })
  
  commodities_date <- reactive({
    
    commodities_filtered <- commodities %>%
      filter(date >= input$date_macro[1],
             date <= input$date_macro[2]) %>% 
      group_by(ticker) %>% 
      mutate(date_index = date - first(date),
             indeksiluku_twr = 100 * (adjusted_dividends_price/
                                        first(adjusted_dividends_price)),
             twr_annualised = if_else(date_index < 250, NA_real_,
                                      100 * ((indeksiluku_twr/100)^(365.25/(as.integer(date_index) + 1)) - 1))) %>% 
      ungroup()
    
    commodities_filtered
    
  })
  
  output$portfolio_return <- renderValueBox({
    
    filtered_grouped <- filtered_data()
    
    return <- prettyNum(round(last(filtered_grouped$indeksiluku_twr) - 100, 2),
                        big.mark = " ",
                        decimal.mark = ",")
    
    valueBox(return, "Portfolion tuotto-%")
  })
  
  output$portfolio_value <- renderValueBox({
    
    filtered_grouped <- filtered_data()
    
    value <- prettyNum(round(last(filtered_grouped$portfolio_value), 1),
                       big.mark = " ",
                       decimal.mark = ",")
    
    valueBox(value, "Portfolion arvo €")
  })
  
  output$largest_return <- renderValueBox({
    
    filtered_stock <- filtered_stock_data()
    
    largest_return <- filtered_stock %>% 
      group_by(name_adj) %>% 
      summarise(return = 100 * (last(adjusted_dividends_price) /
                                  first(adjusted_dividends_price) - 1)) %>% 
      ungroup() %>% 
      filter(return == max(return)) 
    
    return <- prettyNum(round(min(largest_return$return), 2),
                        big.mark = " ",
                        decimal.mark = ",")
    
    valueBox(return, str_c("Suurin tuotto-%: ", unique(largest_return$name_adj)))
  })
  
  output$smallest_return <- renderValueBox({
    
    filtered_stock <- filtered_stock_data()
    
    smallest_return <- filtered_stock %>% 
      group_by(name_adj) %>% 
      summarise(return = 100 * (last(adjusted_dividends_price) /
                                  first(adjusted_dividends_price) - 1)) %>% 
      ungroup() %>% 
      filter(return == min(return)) 
    
    return <- prettyNum(round(min(smallest_return$return), 2),
                        big.mark = " ",
                        decimal.mark = ",")
    
    valueBox(return, str_c("Pienin tuotto-%: ", unique(smallest_return$name_adj)))
  })
  
  output$purchases <- renderValueBox({
    
    filtered_stock <- filtered_stock_data()
    
    purchases <- filtered_stock %>% 
      filter(transaction_type == "buy")
    
    return <- prettyNum(round(replace_na(abs(sum(purchases$transaction_amount_eur)), 0), 2),
                        big.mark = " ",
                        decimal.mark = ",")
    
    valueBox(return, "Ostot €")
  })
  
  output$sells <- renderValueBox({
    
    filtered_stock <- filtered_stock_data()
    
    sells <- filtered_stock %>% 
      filter(transaction_type == "sell")
    
    return <- prettyNum(round(replace_na(abs(sum(sells$transaction_amount_eur)), 0), 2),
                        big.mark = " ",
                        decimal.mark = ",")
    
    valueBox(return, "Myynnit €")
  })
  
  output$returnplot <- renderPlot({
    
    filtered_grouped <- filtered_data()
    
    indices_date <- indices_date()
    
    ## return graph
    
    if (!is.null(input$index)) {
      
      indices <- indices_date %>% 
        filter(index %in% input$index)
      
      df <- filtered_grouped %>% 
        mutate(index = "Portfolio") %>% 
        bind_rows(indices)
      
      return_graafi <- df %>%
        ggplot(aes(x = date)) + ggthemes::theme_economist() +
        theme(legend.position = "bottom",
              legend.title = element_blank(),
              axis.title = element_blank()) +
        geom_line(aes(y = indeksiluku_twr, colour = index)) +
        ggtitle("Portfolion tuoton (€) kehitys")
      
      return_graafi
      
    } else {
      
      return_graafi <- filtered_grouped %>%
        ggplot(aes(x = date)) + ggthemes::theme_economist() +
        theme(legend.position = "none",
              legend.title = element_blank(),
              axis.title = element_blank()) +
        geom_line(aes(y = indeksiluku_twr, colour = "#e3120b")) +
        ggtitle("Portfolion tuoton (€) kehitys")
      
      return_graafi
      
    }
    
  })
  
  output$returnplot_countries <- renderPlot({
    
    filtered_stock <- filtered_stock_data() %>% 
      mutate(amount = quantity_cum*closing_price*market_exchange_rate,
             price_eur = adjusted_dividends_price*market_exchange_rate) %>%
      group_by(ticker) %>% 
      mutate(return = price_eur / first(price_eur) - 1) %>% 
      ungroup() %>% 
      group_by(date) %>% 
      mutate(total_amount = sum(amount)) %>% 
      ungroup() %>% 
      mutate(weight = amount / total_amount) %>% 
      group_by(country, date) %>% 
      summarise(return_country = round(100*weighted.mean(return, weight), 2L)) %>% 
      ungroup()
    
    return_graafi <- filtered_stock %>%
      ggplot(aes(x = date)) + ggthemes::theme_economist() +
      theme(legend.position = "bottom",
            legend.title = element_blank(),
            axis.title = element_blank()) +
      geom_line(aes(y = return_country, colour = country)) +
      ggtitle("Portfolion tuoton (€) kehitys maittain")
    
    return_graafi
    
    
  })
  
  output$returnplot_sectors <- renderPlot({
    
    filtered_stock <- filtered_stock_data() %>% 
      mutate(amount = quantity_cum*closing_price*market_exchange_rate,
             price_eur = adjusted_dividends_price*market_exchange_rate) %>%
      group_by(ticker) %>% 
      mutate(return = price_eur / first(price_eur) - 1) %>% 
      ungroup() %>% 
      group_by(date) %>% 
      mutate(total_amount = sum(amount)) %>% 
      ungroup() %>% 
      mutate(weight = amount / total_amount) %>% 
      group_by(sector_gic, date) %>% 
      summarise(return_sector = round(100*weighted.mean(return, weight), 2L)) %>% 
      ungroup()
    
    return_graafi <- filtered_stock %>%
      ggplot(aes(x = date)) + ggthemes::theme_economist() +
      theme(legend.position = "bottom",
            legend.title = element_blank(),
            axis.title = element_blank()) +
      geom_line(aes(y = return_sector, colour = sector_gic)) +
      ggtitle("Portfolion tuoton (€) kehitys toimialasektoreittain")
    
    return_graafi
    
  })
  
  output$returnplot_stocks <- renderPlot({
    
    filtered_stock <- filtered_stock_data() %>% 
      filter(name_adj %in% input$stock) %>% 
      mutate(price_eur = closing_adjusted_price*market_exchange_rate) %>%
      group_by(ticker) %>% 
      mutate(return = round(100*(price_eur / first(price_eur) - 1), 2L),
             tuotto = indeksiluku_twr - 100) %>% 
      ungroup()
    
    return_graafi <- filtered_stock %>%
      ggplot(aes(x = date)) + ggthemes::theme_economist() +
      theme(legend.position = "bottom",
            legend.title = element_blank(),
            axis.title = element_blank()) +
      geom_line(aes(y = return, colour = name_adj)) +
      ggtitle("Portfolion tuoton (€) kehitys arvopapereittain") +
      ylab(element_blank())
    
    return_graafi
    
  })
  
  output$valueplot <- renderPlot({
    
    filtered_grouped2 <- filtered_data() 
    
    ## value graph
    
    j <- filtered_grouped2 %>%
      ggplot(aes(x = date)) + ggthemes::theme_economist() +
      geom_line(aes(y = portfolio_value, colour = "#e3120b")) +
      theme(legend.position = "none",
            legend.title = element_blank(),
            axis.title = element_blank()) +
      ggtitle("Portfolion arvon (€) kehitys") +
      ylab(element_blank()) +
      scale_y_continuous(label = scales::number_format(big.mark = " ",
                                                       decimal.mark = ","))
    
    j
    
  })
  
  output$allocation_bar1 <- highcharter::renderHighchart({
    
    treemap_data <- treemap_data() %>% 
      arrange(desc(amount))
    
    highchart() %>% 
      hc_add_series(name = "Arvo €", data = treemap_data, type = "bar", 
                    mapping = hcaes(x = name_adj, y = amount, color = country),
                    dataLabels = list(format='{point.y:,.2f}',  enabled = TRUE)) %>% 
      hc_xAxis(categories = treemap_data$name_adj) %>%
      #hc_title("Allokaatio arvopapereittain") %>% 
      hc_add_annotation(labels = treemap_data$amount) %>%  
      hc_add_theme(hc_theme_economist())
    
    
  })
  
  output$barchart_stock_returns <- highcharter::renderHighchart({
    
    filtered_stock <- filtered_stock_data() %>% 
      mutate(price_eur = closing_adjusted_price*market_exchange_rate) %>%
      group_by(ticker) %>% 
      mutate(return = round(100*(price_eur / first(price_eur) - 1), 2L),
             tuotto = indeksiluku_twr - 100) %>% 
      ungroup() %>% 
      filter(date == max(date)) %>%
      arrange(desc(tuotto))
    
    
    highchart() %>% 
      hc_add_series(name = "Tuotto-% (€)", data = filtered_stock, type = "bar", 
                    mapping = hcaes(x = name_adj, y = tuotto, color = country),
                    dataLabels = list(format='{point.y:,.2f}',  enabled = TRUE)) %>% 
      hc_xAxis(categories = filtered_stock$name_adj) %>%
      hc_title(text = "Tuotto-% (€) arvopapereittain",
               margin = 20,
               align = "left",
               style = list(color = "#014d64", useHTML = TRUE)) %>% 
      hc_add_annotation(labels = filtered_stock$tuotto) %>%  
      hc_add_theme(hc_theme_economist())
    
    
  })
  
  output$barchart_industry_returns <- highcharter::renderHighchart({
    
    filtered_stock <- filtered_stock_data() %>% 
      mutate(amount = quantity_cum*closing_price*market_exchange_rate,
             price_eur = adjusted_dividends_price*market_exchange_rate) %>%
      group_by(ticker) %>% 
      mutate(return = price_eur / first(price_eur) - 1) %>% 
      ungroup() %>% 
      group_by(date) %>% 
      mutate(total_amount = sum(amount)) %>% 
      ungroup() %>% 
      mutate(weight = amount / total_amount) %>% 
      group_by(industry_gic, date, sector_gic) %>% 
      summarise(return_industry = round(100*weighted.mean(return, weight), 2L)) %>% 
      ungroup() %>% 
      filter(date == max(date)) %>%
      arrange(desc(return_industry))
    
    #ClickFunction <- JS("function(event) {Shiny.onInputChange('Clicked', event.point.name);}")
    
    
    highchart() %>% 
      hc_add_series(name = "Tuotto-% (€)", data = filtered_stock, type = "bar", 
                    mapping = hcaes(x = industry_gic, y = return_industry, color = sector_gic),
                    dataLabels = list(format='{point.y:,.2f}',  enabled = TRUE)) %>% 
      hc_xAxis(categories = filtered_stock$industry_gic) %>%
      hc_title(text = "Tuotto-% (€) toimialoittain",
               margin = 20,
               align = "left",
               style = list(color = "#014d64", useHTML = TRUE)) %>% 
      hc_add_annotation(labels = filtered_stock$return_industry) %>%  
      hc_add_theme(hc_theme_economist())
    
    
  })
  
  
  treemap_data <- reactive({
    
    filtered_stock <- filtered_stock_data()
    
    df_group <- filtered_stock %>% 
      filter(date == max(date)) %>%
      mutate(amount = round(quantity_cum*closing_price*market_exchange_rate, 2L),
             share = round(100 * amount / sum(amount), 2)) %>% 
      mutate(index = paste(name_adj, amount, share, sep =" \n ")) %>% 
      group_by(industry_gic) %>% 
      mutate(amount2 = round(sum(amount), 2L)) %>% 
      ungroup() %>% 
      mutate(share2 = round(100*amount2/sum(amount), 2L)) %>% 
      group_by(sector_gic) %>% 
      mutate(amount3 = round(sum(amount), 2L)) %>% 
      ungroup() %>% 
      mutate(share3 = round(100*amount3/sum(amount), 2L),
             index2 = paste(industry_gic, amount2, share2, sep = " \n "),
             index3 = paste(sector_gic, amount3, share3, sep = " \n ")) %>% 
      group_by(country) %>% 
      mutate(country_amount = round(sum(amount), 2L)) %>% 
      ungroup() %>% 
      mutate(country_share = round(100*country_amount/sum(amount), 2L),
             country_index = paste(country, country_amount, country_share, sep = " \n "))
  })
  
  output$treemap <- renderD3tree2({
    
    ## treemap

    treemap_data <- treemap_data()
    
    varit <- ggthemes::economist_pal()(9)
    
    p <- treemap(treemap_data,
                 index=c("index3","index2", "index"),
                 #index = "index",
                 vSize = "amount",
                 type="index",
                 #palette=varit,
                 #palette = "Set2",
                 #bg.labels=c("white"),
                 align.labels=list(
                   c("center", "center"), 
                   c("right", "bottom")
                 )  
    ) 

    inter <- d3tree2(p, rootname = "Allokaatio sektoreittain")
    
    inter


  })
  
  output$treemap2 <- renderD3tree2({
    
    ## treemap
    
    treemap_data <- treemap_data()
    
    varit <- ggthemes::economist_pal()(9)
    
    p <- treemap(treemap_data,
                 index=c("country_index", "index"),
                 vSize = "amount",
                 type="index",
                 #palette=varit,
                 #palette = "Set2",
                 #bg.labels=c("white"),
                 align.labels=list(
                   c("center", "center"), 
                   c("right", "bottom")
                 )  
    ) 
    
    inter <- d3tree2(p, rootname = "Allokaatio maittain")
    
    inter
    
    
  })
  
  output$allocation_bar1 <- highcharter::renderHighchart({
    
    treemap_data <- treemap_data() %>% 
      arrange(desc(amount))
    
    highchart() %>% 
      hc_add_series(name = "Arvo €", data = treemap_data, type = "bar", 
                    mapping = hcaes(x = name_adj, y = amount, color = country),
                    dataLabels = list(format='{point.y:,.2f}',  enabled = TRUE)) %>% 
      hc_xAxis(categories = treemap_data$name_adj) %>%
      hc_title(text = "Allokaatio arvopapereittain",
               margin = 20,
               align = "left",
               style = list(color = "#014d64", useHTML = TRUE)) %>% 
      hc_add_annotation(labels = treemap_data$amount) %>%  
      hc_add_theme(hc_theme_economist())
    
    
  })
  
  output$allocation_bar2 <- highcharter::renderHighchart({
    
    treemap_data <- treemap_data() %>% 
      distinct(sector_gic, industry_gic, amount2) %>% 
      arrange(desc(amount2))
    
    highchart() %>% 
      hc_add_series(name = "Arvo €", data = treemap_data, type = "bar", 
                    mapping = hcaes(x = industry_gic, y = amount2, color = sector_gic),
                    dataLabels = list(format='{point.y:,.2f}',  enabled = TRUE)) %>% 
      hc_title(text = "Allokaatio toimialoittain",
               margin = 20,
               align = "left",
               style = list(color = "#014d64", useHTML = TRUE)) %>% 
      hc_xAxis(categories = treemap_data$industry_gic) %>%
      hc_add_annotation(labels = treemap_data$amount2) %>%  
      hc_add_theme(hc_theme_economist())
    
    
  })
  
  output$map <- renderLeaflet({
    
    filtered_stock <- filtered_stock_data() %>% 
      filter(!is.na(lat)) %>% 
      group_by(ticker) %>% 
      filter(date == max(date)) %>% 
      ungroup() %>% 
      mutate(tuotto = indeksiluku_twr - 100)
    
    bins <- c(-100, -50, -30, -20, -10, 0, 10, 20, 50, 100, Inf)
    pal <- colorBin("RdYlBu", domain = filtered_stock$indeksiluku_twr, bins = bins)
    
    ## value graph
    
    leaflet(filtered_stock) %>% 
      addTiles() %>%
      addCircleMarkers(lng = ~lon, lat = ~lat,
                       radius = 6.5,
                       fillColor = ~pal(filtered_stock$tuotto),
                       stroke = FALSE, fillOpacity = 1,
                       label = map(paste0('Yritys: ', filtered_stock$name_adj, '<p></p>',
                                          'Osoite: ', filtered_stock$address, '<p></p>',
                                          'Tuotto-%: ', prettyNum(round(filtered_stock$tuotto, 2),
                                                                  big.mark = " ",
                                                                  decimal.mark = ","), '</p>'),
                                   htmltools::HTML)) %>% 
      addLegend("bottomright",
                pal = pal,
                values = ~filtered_stock$tuotto,
                title = "Tuotto-%",
                labFormat = labelFormat(suffix = "%"),
                opacity = 1)
    
    
  })
  
  output$indices_plot <- renderPlot({
    
    indices_date <- indices_date2()
    
    indices <- indices_date %>% 
      filter(index %in% input$index_macro)
    
    ## return graph
    
    if (input$annualisoitu == "Tavallinen") {
      
      return_graafi <- indices %>%
        ggplot(aes(x = date)) + 
        ggthemes::theme_economist() +
        theme(legend.position = "bottom",
              legend.title = element_blank(),
              axis.title = element_blank()) +
        geom_line(aes(y = indeksiluku_twr, colour = index)) +
        ggtitle("Indeksien kehitys")
      
      return_graafi
      
    } else if (input$annualisoitu == "Annualisoitu") {
      
      return_graafi <- indices %>%
        ggplot(aes(x = date)) + 
        ggthemes::theme_economist() +
        theme(legend.position = "bottom",
              legend.title = element_blank(),
              axis.title = element_blank()) +
        geom_line(aes(y = twr_annualised, colour = index)) +
        ggtitle("Indeksien kehitys")
      
      return_graafi
      
    }
    
  })
  
  output$commodities_plot <- renderPlot({
    
    commodities_date <- commodities_date()
    
    commodities <- commodities_date %>% 
      filter(commodity %in% input$commodity)
    
    ## return graph
    
    if (input$annualisoitu == "Tavallinen") {
      
      return_graafi <- commodities %>%
        ggplot(aes(x = date)) + 
        ggthemes::theme_economist() +
        theme(legend.position = "bottom",
              legend.title = element_blank(),
              axis.title = element_blank()) +
        geom_line(aes(y = indeksiluku_twr, colour = commodity)) +
        ggtitle("Raaka-aineiden kehitys")
      
      return_graafi
      
    } else if (input$annualisoitu == "Annualisoitu") {
      
      return_graafi <- commodities %>%
        ggplot(aes(x = date)) + 
        ggthemes::theme_economist() +
        theme(legend.position = "bottom",
              legend.title = element_blank(),
              axis.title = element_blank()) +
        geom_line(aes(y = twr_annualised, colour = commodity)) +
        ggtitle("Raaka-aineiden kehitys")
      
      return_graafi
      
    }
    
  })
  
  
  output$tunnusluvut_table <- renderDataTable(
    
    df_fundamentals_financials %>% 
      filter(ticker %in% !!filtered_stock_data()$ticker) %>% 
      transmute(ticker,
                PETrailing = TrailingPE,
                PEForward = ForwardPE,
                PriceToSales = PriceSalesTTM,
                PriceToBook = PriceBookMRQ,
                `EV / Sales` = EnterpriseValueRevenue,
                `EV / EBITDA` = EnterpriseValueEbitda,
                Beta,
                ROE = ReturnOnEquityTTM * 100,
                ROA = ReturnOnAssetsTTM * 100,
                OperatingMargin = OperatingMarginTTM * 100,
                MarketCap = MarketCapitalizationMln,
                EPS = EarningsShare,
                EPSDiluted = DilutedEpsTTM,
                QuarterlyRevenueGrowthYOY,
                QuarterlyEarningsGrowthYOY,
                DividendShare,
                DividendYield,
                InsidersRatio = PercentInsiders,
                InstitutionsRatio = PercentInstitutions,
                ShortRatio = ShortRatio
      )
    
  )
  
  
}

shinyApp(ui = ui, server = server, options = list(port = 3838))
