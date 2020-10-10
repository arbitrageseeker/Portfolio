FROM rocker/shiny-verse:3.5  

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
    libclang-3.8-dev \
    libobjc-6-dev \
    libclang1-3.8 \
    libclang-common-3.8-dev \
    libllvm3.8 \
    libobjc4 \
    libgc1c2 \
    python-pip \
    dialog \
    nano \
    && pip install virtualenv \
    && pip install awscli    

RUN R -e "install.packages(c('tidyverse', 'lubridate', 'shiny','shinydashboard','scales','shinythemes','ggthemes', 'shinyWidgets', 'glue', 'leaflet', 'aws.s3'), repos='http://cran.rstudio.com/')"

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
