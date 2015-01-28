Vitess YCSB Driver
==================

Test using the command line unit test tool.
  ```sh
  mvn clean package
  java -cp vitess/target/vitess-binding-0.1.4.jar com.yahoo.ycsb.CommandLine \
      -db com.yahoo.ycsb.VitessClient \
      -p hosts=localhost:15007 \
      -p keyspace=test_keyspace \
      -p createTable="create table usertable (pri_key varbinary(50), first varbinary(50), last varbinary(50), keyspace_id varbinary(50) NOT NULL, primary key (pri_key)) Engine=InnoDB" \
      -p dropTable="drop table if exists usertable"
  insert brianfrankcooper first=brian last=cooper
  read brianfrankcooper
  ```
