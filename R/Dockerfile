FROM rocker/geospatial:latest

RUN apt-get update && apt-get install -y \
    software-properties-common

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    file \
    git \
    libapparmor1 \
    libcurl4-openssl-dev \
    libedit2 \
    libssl-dev \
    lsb-release \
    psmisc \
    procps \
    python-setuptools \
    sudo \
    wget \
    dialog \
    nano \
&& install2.r --error \
    keyring \
    tidyquant \
    tmaptools \
    Quandl \
    rootSolve \
    aws.s3

COPY .Renviron /home/rstudio/.Renviron

RUN mkdir /home/rstudio/r

# Copy scripts to the image
COPY transactions.R /home/rstudio/r/transactions.R
COPY fundamentals.R /home/rstudio/r/fundamentals.R
COPY dividends_and_splits.R /home/rstudio/r/dividends_and_splits.R
COPY daily_prices.R /home/rstudio/r/daily_prices.R
COPY data_proc.R /home/rstudio/r/data_proc.R
COPY make.R /home/rstudio/r/make.R

WORKDIR /home/rstudio

# Execute the Container
CMD R -e "source('/home/rstudio/r/make.R')"
