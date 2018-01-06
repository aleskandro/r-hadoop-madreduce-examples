FROM sequenceiq/hadoop-docker:2.7.0

## Fix for yum with Docker
RUN yum -y install yum-plugin-ovl || true

# Installing R
RUN rpm -Uvh http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
RUN yum -y install R

# Dev tools for R
RUN yum -y install openssl-devel libcurl-devel libxml2-devel
RUN R -e 'install.packages("devtools", repos="https://cran.stat.unipd.it/")'
RUN R -e 'install.packages("roxygen2", repos="https://cran.stat.unipd.it/")'
RUN R -e "devtools::install_github('klmr/modules')"

#### Installing RHadoop

ENV HADOOP_CMD=/usr/local/hadoop/bin/hadoop 
ENV HADOOP_STREAMING="/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.0.jar" 
RUN R -e "devtools::install_github(c('RevolutionAnalytics/rmr2/pkg', 'RevolutionAnalytics/plyrmr/pkg', 'RevolutionAnalytics/rhdfs/pkg'))"

#### RHipe

ENV HADOOP_BIN=/usr/local/hadoop/bin/hadoop 
RUN R -e 'install.packages("datadr", repos="https://cran.stat.unipd.it/")'
RUN yum -y install ftp://ftp.pbone.net/mirror/ftp5.gwdg.de/pub/opensuse/repositories/home:/mrdocs:/protobuf-rpm/CentOS_CentOS-6/x86_64/libprotobuf7-2.4.1-6.2.x86_64.rpm ftp://ftp.pbone.net/mirror/ftp5.gwdg.de/pub/opensuse/repositories/home:/mrdocs:/protobuf-rpm/CentOS_CentOS-6/x86_64/libprotoc7-2.4.1-6.2.x86_64.rpm ftp://ftp.pbone.net/mirror/ftp5.gwdg.de/pub/opensuse/repositories/home:/mrdocs:/protobuf-rpm/CentOS_CentOS-6/x86_64/protobuf-devel-2.4.1-6.2.x86_64.rpm ftp://ftp.pbone.net/mirror/ftp5.gwdg.de/pub/opensuse/repositories/home:/mrdocs:/protobuf-rpm/CentOS_CentOS-6/x86_64/protobuf-java-2.4.1-6.2.x86_64.rpm ftp://ftp.pbone.net/mirror/ftp5.gwdg.de/pub/opensuse/repositories/home:/mrdocs:/protobuf-rpm/CentOS_CentOS-6/x86_64/libprotobuf-lite7-2.4.1-6.2.x86_64.rpm

RUN curl http://ml.stat.purdue.edu/rhipebin/archive/Rhipe_0.72.tar.gz > /tmp/rhipe.tar.gz
RUN R CMD INSTALL /tmp/rhipe.tar.gz
RUN rm -rf /tmp/rhipe.tar.gz

#### Final environment

RUN mkdir /code
WORKDIR /code
ENV PATH="/usr/local/hadoop/bin:${PATH}"

CMD [ "/etc/bootstrap.sh", "-d" ]
