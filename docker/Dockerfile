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

ENV HADOOP_BIN=/usr/local/hadoop/bin/
RUN R -e 'install.packages("datadr", repos="https://cran.stat.unipd.it/")'

RUN yum install -y ftp://ftp.pbone.net/mirror/ftp5.gwdg.de/pub/opensuse/repositories/home:/kalyaka/CentOS_CentOS-6/x86_64/protobuf-2.5.0-16.1.x86_64.rpm ftp://ftp.pbone.net/mirror/ftp5.gwdg.de/pub/opensuse/repositories/home:/kalyaka/CentOS_CentOS-6/x86_64/protobuf-compiler-2.5.0-16.1.x86_64.rpm ftp://ftp.pbone.net/mirror/ftp5.gwdg.de/pub/opensuse/repositories/home:/kalyaka/CentOS_CentOS-6/x86_64/protobuf-debuginfo-2.5.0-16.1.x86_64.rpm ftp://ftp.pbone.net/mirror/ftp5.gwdg.de/pub/opensuse/repositories/home:/kalyaka/CentOS_CentOS-6/x86_64/protobuf-devel-2.5.0-16.1.x86_64.rpm ftp://ftp.pbone.net/mirror/ftp5.gwdg.de/pub/opensuse/repositories/home:/kalyaka/CentOS_CentOS-6/x86_64/protobuf-lite-2.5.0-16.1.x86_64.rpm ftp://ftp.pbone.net/mirror/ftp5.gwdg.de/pub/opensuse/repositories/home:/kalyaka/CentOS_CentOS-6/x86_64/protobuf-lite-devel-2.5.0-16.1.x86_64.rpm 

RUN curl http://ml.stat.purdue.edu/rhipebin/Rhipe_0.75.2_hadoop-2.tar.gz > /tmp/rhipe.tar.gz
RUN R CMD INSTALL /tmp/rhipe.tar.gz
RUN rm -rf /tmp/rhipe.tar.gz

RUN R -e 'install.packages("testthat", repos="https://cran.stat.unipd.it/")'

#### Final environment

RUN mkdir /code
WORKDIR /code
ENV PATH="/usr/local/hadoop/bin:${PATH}"
#RUN echo 'export HADOOP_LIBS=$(hadoop classpath | tr -d "*")' >> /etc/bootstrap.sh
ENV HADOOP_LIBS=/usr/local/hadoop/etc/hadoop/:/usr/local/hadoop/share/hadoop/common/lib/:/usr/local/hadoop/share/hadoop/common/:/usr/local/hadoop/share/hadoop/hdfs:/usr/local/hadoop/share/hadoop/hdfs/lib/:/usr/local/hadoop/share/hadoop/hdfs/:/usr/local/hadoop/share/hadoop/yarn/lib/:/usr/local/hadoop/share/hadoop/yarn/:/usr/local/hadoop/share/hadoop/mapreduce/lib/:/usr/local/hadoop/share/hadoop/mapreduce/:/usr/local/hadoop/contrib/capacity-scheduler/

CMD [ "/etc/bootstrap.sh", "-d" ]
