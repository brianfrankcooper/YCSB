# Getting Started on Linux

1. Install Java and Maven

  Go to [Java SE Downloads page](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
  and get the URL to download the RPM, e.g.:

  ```sh
  wget http://download.oracle.com/otn-pub/java/jdk/7u40-b43/jdk-7u40-linux-x64.rpm?AuthParam=11232426132 -o jdk-7u40-linux-x64.rpm
  rpm -Uvh jdk-7u40-linux-x64.rpm
  ```

  Or install via `yum` or `apt-get`:

  ```sh
  sudo yum install java-devel
  ```

  Download and install [Maven](http://maven.apache.org/download.cgi), e.g.:

  ```sh
  wget http://ftp.heanet.ie/mirrors/www.apache.org/dist/maven/maven-3/3.1.1/binaries/apache-maven-3.1.1-bin.tar.gz
  sudo tar xzf apache-maven-*-bin.tar.gz -C /usr/local
  cd /usr/local
  sudo ln -s apache-maven-* maven
  sudo vi /etc/profile.d/maven.sh
  ```

  Add the following to `maven.sh`:

  ```sh
  export M2_HOME=/usr/local/maven
  export PATH=${M2_HOME}/bin:${PATH}
  ```

  Reload `bash` and test `mvn`:

  ```sh
  bash
  mvn -version
  ```

1. Download the [latest release of YCSB](https://github.com/brianfrankcooper/YCSB/releases/latest):

  ```sh
  curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.5.0/ycsb-0.5.0.tar.gz
  tar xfvz ycsb-0.5.0.tar.gz
  cd ycsb-0.5.0
  ```

1. Set up a database to benchmark. There is a README file under each binding
   directory.

1. Run YCSB command.
    
  ```sh
  bin/ycsb load basic -P workloads/workloada
  bin/ycsb run basic -P workloads/workloada
  ```

  Running the `ycsb` command without any argument will print the usage. 
   
  See https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload
  for a detailed documentation on how to run a workload.

  See https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties for 
  the list of available workload properties.

Building from source
--------------------

YCSB requires the use of Maven 3; if you use Maven 2, you may see [errors
such as these](https://github.com/brianfrankcooper/YCSB/issues/406).

To build the full distribution, with all database bindings:

    mvn clean package

To build a single database binding:

    mvn -pl com.yahoo.ycsb:mongodb-binding -am clean package
