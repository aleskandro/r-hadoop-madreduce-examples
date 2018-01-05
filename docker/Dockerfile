FROM sequenceiq/hadoop-docker:2.7.0
RUN yum -y install yum-plugin-ovl || true
RUN rpm -Uvh http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
RUN yum -y install R
RUN yum -y install openssl-devel libcurl-devel libxml2-devel
RUN R -e 'install.packages("devtools", repos="https://cran.stat.unipd.it/")'
RUN R -e 'install.packages("roxygen2", repos="https://cran.stat.unipd.it/")'
RUN R -e "devtools::install_github('klmr/modules')"

RUN mkdir /code
WORKDIR /code
ENV PATH="/usr/local/hadoop/bin:${PATH}"

CMD [ "/etc/bootstrap.sh", "-d" ]
