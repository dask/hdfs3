FROM ubuntu:trusty

# conda
RUN apt-get update && apt-get install -y -q curl bzip2
RUN curl http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
RUN /bin/bash /tmp/miniconda.sh -b -p /opt/conda
RUN rm /tmp/miniconda.sh
ENV PATH /opt/conda/bin:$PATH

# libhdfs
RUN apt-get install -y -q git build-essential cmake libxml2 libxml2-dev uuid-dev protobuf-compiler libprotobuf-dev libgsasl7-dev libkrb5-dev libboost1.54-all-dev
# RUN git clone https://github.com/PivotalRD/libhdfs3.git /opt/libhdfs3
# ADD docker-files/libhdfs-build.sh /tmp/libhdfs-build.sh
# RUN bash /tmp/libhdfs-build.sh
ENV LD_LIBRARY_PATH /usr/local/lib:$LD_LIBRARY_PATH
ENV LIBHDFS3_CONF /etc/hadoop/conf/hdfs-site.xml

# hdfs3 - python
RUN /opt/conda/bin/conda install -y -q ipython pytest
RUN conda install libhdfs3 -c blaze
# RUN /opt/conda/bin/pip install versioneer pywebhdfs click

# Cloudera repositories
RUN curl -s http://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh/archive.key | apt-key add -
RUN echo 'deb [arch=amd64] http://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh trusty-cdh5 contrib' > /etc/apt/sources.list.d/cloudera.list
RUN echo 'deb-src http://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh trusty-cdh5 contrib' >> /etc/apt/sources.list.d/cloudera.list
ADD docker-files/cloudera.pref /etc/apt/preferences.d/cloudera.pref

# Install CDH5 in a single node: Pseudo Distributed
ADD docker-files/cdh5-install.sh /tmp/cdh5-install.sh
RUN bash /tmp/cdh5-install.sh

EXPOSE 8020
EXPOSE 50070

VOLUME /hdfs3
WORKDIR /hdfs3

ADD docker-files/start.sh /tmp/start.sh
CMD ["bash", "/tmp/start.sh"]
