FROM openjdk:8-bullseye

RUN apt-get update && \
    apt-get install --no-install-recommends -y \
        python-is-python2 \
        less locales strace lsof vim nano iproute2 tmux make openssl libssl-dev curl && \
    apt-get clean

# set default java8
RUN update-alternatives --install "/usr/bin/java" "java" "/usr/local/openjdk-8/bin/java" 1 && \
    update-alternatives --set java "/usr/local/openjdk-8/bin/java"

# locale
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    locale-gen && \
    echo 'LANG="en_US.UTF-8"' > /etc/default/locale && \
    echo '\nexport LANG="en_US.UTF-8"\nexport LANGUAGE="en_US.UTF-8"\nexport LC_ALL="en_US.UTF-8"' >> /etc/profile
ENV LANG="en_US.UTF-8" LANGUAGE="en_US.UTF-8" LC_ALL="en_US.UTF-8"

WORKDIR /hpqs

# download hbase + phoenix + phoenix-queryserver
RUN curl -o "hbase-1.4.14-bin.tar.gz" "http://archive.apache.org/dist/hbase/1.4.14/hbase-1.4.14-bin.tar.gz" && \
    curl -o "apache-phoenix-4.15.0-HBase-1.4-bin.tar.gz" "https://dlcdn.apache.org/phoenix/apache-phoenix-4.15.0-HBase-1.4/bin/apache-phoenix-4.15.0-HBase-1.4-bin.tar.gz" && \
    curl -o "phoenix-queryserver-6.0.0-bin.tar.gz" "https://dlcdn.apache.org/phoenix/phoenix-queryserver-6.0.0/phoenix-queryserver-6.0.0-bin.tar.gz" && \
    tar -xzf hbase-1.4.14-bin.tar.gz && \
    tar -xzf apache-phoenix-4.15.0-HBase-1.4-bin.tar.gz && \
    tar -xzf phoenix-queryserver-6.0.0-bin.tar.gz && \
    rm hbase-1.4.14-bin.tar.gz && \
    rm apache-phoenix-4.15.0-HBase-1.4-bin.tar.gz && \
    rm phoenix-queryserver-6.0.0-bin.tar.gz

RUN cp /hpqs/apache-phoenix-4.15.0-HBase-1.4-bin/phoenix-4.15.0-HBase-1.4-server.jar /hpqs/hbase-1.4.14/lib/

ADD develop/hpqs/start.sh /hpqs/start.sh
