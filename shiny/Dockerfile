FROM rocker/shiny-verse:latest 

# Logging path
RUN mkdir -p /var/lib/shiny-server/bookmarks/shiny

# Install presequities
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
    libclang-dev \
    libclang-dev \
    libobjc-7-dev \
    libclang1\
    libclang-common-8-dev \
    libobjc4 \
    libgc1c2 \
    software-properties-common

RUN add-apt-repository universe 

RUN apt-get update \
&& apt-get install -y --no-install-recommends \
    python3-pip \
    dialog \
    nano \
    && pip3 install virtualenv \
    && pip3 install awscli    

RUN R -e "install.packages(c('tidyverse', 'remotes', 'lubridate', 'shiny','shinydashboard','scales','shinythemes','ggthemes', 'shinyWidgets', 'glue', 'leaflet', 'aws.s3', 'treemap', 'highcharter'), repos='http://cran.rstudio.com/')"

RUN R -e "remotes::install_github('d3treeR/d3treeR')"

COPY .Renviron /home/shiny/.Renviron
RUN chown shiny.shiny /home/shiny/.Renviron

# Copy custom shiny-server.sh for environment variables
COPY shiny-server.sh /usr/bin/shiny-server.sh
RUN chmod 755 /usr/bin/shiny-server.sh

# Copy the app to the image
COPY app.R /srv/shiny-server/

# Make all app files readable
RUN chmod -R 755 /srv/shiny-server/

# Avoid using root, so use the pre-built user shiny
USER shiny

# Port for our Shiny WebApp
EXPOSE 3838

# Execute the Container
CMD ["/usr/bin/shiny-server.sh"]
