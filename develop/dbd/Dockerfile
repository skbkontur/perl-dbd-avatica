FROM perl:5.34-buster

RUN apt-get update && \
    apt-get install --no-install-recommends -y \
        less locales strace lsof vim nano iproute2 tmux \
        libprotobuf-dev libprotoc-dev g++ libstdc++6 libstdc++-8-dev make cmake openssl libssl-dev spell && \
    apt-get clean

# locale
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    locale-gen && \
    echo 'LANG="en_US.UTF-8"' > /etc/default/locale && \
    echo '\nexport LANG="en_US.UTF-8"\nexport LANGUAGE="en_US.UTF-8"\nexport LC_ALL="en_US.UTF-8"' >> /etc/profile
ENV LANG="en_US.UTF-8" LANGUAGE="en_US.UTF-8" LC_ALL="en_US.UTF-8"

# some magic to install Google::ProtocolBuffers::Dynamic
RUN export PATH=$PATH:$(dirname $(${CCPREFIX}gcc -print-prog-name=cc1))
RUN ln -s /usr/lib/$(gcc -dumpmachine)/libstdc++.so.6 /usr/lib/$(gcc -dumpmachine)/libstdc++.so

# perl modules
ADD . /dbd
WORKDIR /dbd
RUN cpanm -n --installdeps --with-develop /dbd
RUN dzil authordeps --missing | cpanm
