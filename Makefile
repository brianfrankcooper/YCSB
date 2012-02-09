#
# Running 'make -f Makefile.download-db-libs' will automatically download all 
# of the libraries needed to build the YCSB drivers.
#
CASSANDRA_5_DIR=db/cassandra-0.5/lib
CASSANDRA_5_FILE=apache-cassandra-0.5.1-bin.tar.gz
CASSANDRA_6_DIR=db/cassandra-0.6/lib
CASSANDRA_6_FILE=apache-cassandra-0.6.13-bin.tar.gz
CASSANDRA_7_DIR=db/cassandra-0.7/lib
CASSANDRA_7_FILE=apache-cassandra-0.7.9-bin.tar.gz
CASSANDRA_8_DIR=db/cassandra-0.8/lib
CASSANDRA_8_FILE=apache-cassandra-0.8.7-bin.tar.gz
CASSANDRA_1X_VERSION=1.0.6
CASSANDRA_1X_DIR=db/cassandra-$(CASSANDRA_1X_VERSION)/lib
CASSANDRA_1X_FILE=apache-cassandra-$(CASSANDRA_1X_VERSION)-bin.tar.gz
HBASE_DIR=db/hbase/lib
HBASE_VERSION=0.90.5
HBASE_FILE=hbase-$(HBASE_VERSION).tar.gz
INFINISPAN_DIR=db/infinispan-5.0/lib
INFINISPAN_FILE=infinispan-5.0.0.CR8-bin.zip
MONGODB_DIR=db/mongodb/lib
MONGODB_FILE=mongo-2.7.2.jar
REDIS_DIR=db/redis/lib
REDIS_FILE=jedis-2.0.0.jar
VOLDEMORT_DIR=db/voldemort/lib
VOLDEMORT_FILE=voldemort-0.90.1.tar.gz
MAPKEEPER_DIR=db/mapkeeper/lib
MAPKEEPER_FILE=mapkeeper.jar
GEMFIRE_DIR=db/gemfire/lib
GEMFIRE_FILE=gemfire-6.6.1.jar
NOSQLDB_DIR=db/nosqldb/lib
NOSQLDB_FILE=kv-ce-1.2.123.tar.gz

.PHONY: build
build: download-database-deps
	ant -q -e compile
	grep name=\"dbcompile build.xml | perl -ne '$$_=~/name=\"(.+)\"\s+depends/; print "$$1\n"; system "ant -q -e $$1"'

download-database-deps:  $(CASSANDRA_5_DIR)/$(CASSANDRA_5_FILE) \
			 $(CASSANDRA_6_DIR)/$(CASSANDRA_6_FILE) \
			 $(CASSANDRA_7_DIR)/$(CASSANDRA_7_FILE) \
			 $(CASSANDRA_8_DIR)/$(CASSANDRA_8_FILE) \
			 $(CASSANDRA_1X_DIR)/$(CASSANDRA_1X_FILE) \
			 $(HBASE_DIR)/$(HBASE_FILE)             \
			 $(INFINISPAN_DIR)/$(INFINISPAN_FILE)   \
			 $(MONGODB_DIR)/$(MONGODB_FILE)   \
			 $(REDIS_DIR)/$(REDIS_FILE)   \
			 $(VOLDEMORT_DIR)/$(VOLDEMORT_FILE)   \
			 $(MAPKEEPER_DIR)/$(MAPKEEPER_FILE)   \
			 $(GEMFIRE_DIR)/$(GEMFIRE_FILE)   \
			 $(NOSQLDB_DIR)/$(NOSQLDB_FILE)   \

$(CASSANDRA_5_DIR)/$(CASSANDRA_5_FILE) :
	wget http://archive.apache.org/dist/cassandra/0.5.1/$(CASSANDRA_5_FILE)\
		 -O $(CASSANDRA_5_DIR)/$(CASSANDRA_5_FILE)
	tar -C $(CASSANDRA_5_DIR) -zxf $(CASSANDRA_5_DIR)/$(CASSANDRA_5_FILE)

$(CASSANDRA_6_DIR)/$(CASSANDRA_6_FILE) :
	wget http://archive.apache.org/dist/cassandra/0.6.13/$(CASSANDRA_6_FILE)\
		 -O $(CASSANDRA_6_DIR)/$(CASSANDRA_6_FILE)
	tar -C $(CASSANDRA_6_DIR) -zxf $(CASSANDRA_6_DIR)/$(CASSANDRA_6_FILE)

$(CASSANDRA_7_DIR)/$(CASSANDRA_7_FILE) :
	wget http://archive.apache.org/dist/cassandra/0.7.9/$(CASSANDRA_7_FILE)\
		 -O $(CASSANDRA_7_DIR)/$(CASSANDRA_7_FILE)
	tar -C $(CASSANDRA_7_DIR) -zxf $(CASSANDRA_7_DIR)/$(CASSANDRA_7_FILE)

$(CASSANDRA_8_DIR)/$(CASSANDRA_8_FILE) :
	wget http://archive.apache.org/dist/cassandra/0.8.7/$(CASSANDRA_8_FILE)\
		 -O $(CASSANDRA_8_DIR)/$(CASSANDRA_8_FILE)
	tar -C $(CASSANDRA_8_DIR) -zxf $(CASSANDRA_8_DIR)/$(CASSANDRA_8_FILE)

$(CASSANDRA_1X_DIR)/$(CASSANDRA_1X_FILE) :
	wget http://archive.apache.org/dist/cassandra/$(CASSANDRA_1X_VERSION)/$(CASSANDRA_1X_FILE)\
		 -O $(CASSANDRA_1X_DIR)/$(CASSANDRA_1X_FILE)
	tar -C $(CASSANDRA_1X_DIR) -zxf $(CASSANDRA_1X_DIR)/$(CASSANDRA_1X_FILE)

$(HBASE_DIR)/$(HBASE_FILE) :
	wget http://archive.apache.org/dist/hbase/hbase-$(HBASE_VERSION)/$(HBASE_FILE)\
		 -O $(HBASE_DIR)/$(HBASE_FILE)
	tar -C $(HBASE_DIR) -zxf $(HBASE_DIR)/$(HBASE_FILE)

$(INFINISPAN_DIR)/$(INFINISPAN_FILE) :
	wget http://iweb.dl.sourceforge.net/project/infinispan/infinispan/5.0.0.CR8/$(INFINISPAN_FILE)\
		 -O $(INFINISPAN_DIR)/$(INFINISPAN_FILE)
	unzip -a $(INFINISPAN_DIR)/$(INFINISPAN_FILE) -d $(INFINISPAN_DIR)

$(MONGODB_DIR)/$(MONGODB_FILE) :
	wget https://github.com/downloads/mongodb/mongo-java-driver/$(MONGODB_FILE)\
		 -O $(MONGODB_DIR)/$(MONGODB_FILE)

$(REDIS_DIR)/$(REDIS_FILE) :
	wget https://github.com/downloads/xetorthio/jedis/$(REDIS_FILE)\
		 -O $(REDIS_DIR)/$(REDIS_FILE)

$(VOLDEMORT_DIR)/$(VOLDEMORT_FILE) :
	wget https://github.com/downloads/voldemort/voldemort/$(VOLDEMORT_FILE)\
		 -O $(VOLDEMORT_DIR)/$(VOLDEMORT_FILE)
	tar -C $(VOLDEMORT_DIR) -zxf $(VOLDEMORT_DIR)/$(VOLDEMORT_FILE)

$(MAPKEEPER_DIR)/$(MAPKEEPER_FILE) :
	wget https://raw.github.com/m1ch1/mapkeeper/master/lib/mapkeeper.jar \
		 -O $(MAPKEEPER_DIR)/$(MAPKEEPER_FILE)
	wget https://raw.github.com/m1ch1/mapkeeper/master/lib/libthrift-0.6.1.jar \
		 -O $(MAPKEEPER_DIR)/libthrift-0.6.1.jar

$(GEMFIRE_DIR)/$(GEMFIRE_FILE) :
	wget http://dist.gemstone.com.s3.amazonaws.com/maven/release/com/gemstone/gemfire/gemfire/6.6.1/$(GEMFIRE_FILE) \
		 -O $(GEMFIRE_DIR)/$(GEMFIRE_FILE)

$(NOSQLDB_DIR)/$(NOSQLDB_FILE) :
	wget http://download.oracle.com/otn/nosql-database/$(NOSQLDB_FILE) \
		 -O $(NOSQLDB_DIR)/$(NOSQLDB_FILE)
	tar -C $(NOSQLDB_DIR) -zxf $(NOSQLDB_DIR)/$(NOSQLDB_FILE)



