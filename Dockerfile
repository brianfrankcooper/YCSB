# Development Docker image for YCSB, with JDK, & Maven
#
# To build Docker image run following command (in repository root):
#   docker build -t ycsb-dev .
#
# To then build the full distribution of YCSB, run the following command:
#   docker run --rm -v $(pwd):/usr/ycsb ycsb-dev mvn clean package
#
# To then build just a single database binding (eg. cassandra-ts), run the following command:
#   docker run --rm -v $(pwd):/usr/ycsb ycsb-dev mvn -pl cassandra-ts -am clean package
#

FROM maven:3.6-jdk-8

# Configure the main working directory. This is the base
# directory used in any further RUN, COPY, and ENTRYPOINT
# commands.
RUN mkdir -p /usr/ycsb
WORKDIR /usr/ycsb

# Copy the main application.
COPY . /usr/ycsb

# Compile KeepAlive Loop
RUN javac -d /usr/sbin KeepAlive.java

# Run the KeepAlive Loop on starting the container
CMD cd /usr/sbin && java KeepAlive
