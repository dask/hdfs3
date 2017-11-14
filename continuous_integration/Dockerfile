FROM ubuntu:trusty

# conda
RUN apt-get update && apt-get install -y -q curl bzip2
RUN curl https://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
RUN /bin/bash /tmp/miniconda.sh -b -p /opt/conda
RUN rm /tmp/miniconda.sh
ENV PATH /opt/conda/bin:$PATH

# hdfs3 - python
ENV LIBHDFS3_CONF /etc/hadoop/conf/hdfs-site.xml
RUN conda install -y -q ipython pytest flake8 -c conda-forge
RUN conda install -y -q libhdfs3 -c conda-forge

RUN conda create -y -n py3 python=3.5
RUN conda install -y -n py3 ipython pytest flake8 -c conda-forge
RUN conda install -y -n py3 libhdfs3 -c conda-forge

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
