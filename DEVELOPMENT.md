# INSTALL DEPENDENCIES manually (based on DEBIAN/UBUNTU packages names)

## OS-LEVEL DEPENDENCES

Install packages:

    apt-get install libprotobuf-dev libprotoc-dev g++ make cmake openssl libssl-dev libstdc++6 libstdc++-8-dev # (or more newer libstdc++-<NUMBER>-dev)

Add path to `cc1` to env var `$PATH`:

    export PATH=$PATH:$(dirname $(${CCPREFIX}gcc -print-prog-name=cc1))

Add symbolic link from `libstdc++.so.6` to `libstdc++.so`:

    ln -s /usr/lib/x86_64-linux-gnu/libstdc++.so.6 /usr/lib/x86_64-linux-gnu/libstdc++.so

Install package for spell check:

    apt-get install spell

## INSTALL CPAN MODULES

    cpanm --installdeps --with-develop .        # module dependencies
    dzil authordeps --missing | cpanm           # to install dzil dependencies

# INSTALL DEPENDENCIES via docker-compose

    docker-compose up -d                        # build and run

    docker-compose exec app bash                # to get inside the container with perl
    docker-compose exec hpqs bash               # to get inside the hbase+phoenix+queryserver container
    docker-compose exec hpqs /hpqs/hbase-1.4.14/bin/hbase shell                        # to get hbase shell
    docker-compose exec hpqs /hpqs/apache-phoenix-4.15.0-HBase-1.4-bin/bin/sqlline.py  # to get phoenix console

# SETUP

    dzil setup

# RUN TESTS

    prove -l t
    dzil test

Use environment variable TEST_ONLINE=http://avatica.server:1234 to make integrational tests or set TEST_ONLINE to 0 to mock user agent requests.

    TEST_ONLINE=http://hpqs:8765 prove -lv t     # integrational tests with real requests
    TEST_ONLINE=0 prove -lv t                    # mock/unit tests with no real requests

# BUILD

    dzil build

# RELEASE/UPLOAD TO CPAN

    # write changes to "Changes" file
    # update version in DBD::Avatica::VERSION and in other files
    dzil release
