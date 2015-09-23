Quick Start
===============
### 1. Set Up YCSB

Download the YCSB from this website:

    https://github.com/brianfrankcooper/YCSB/releases/

You can choose to download either the full stable version or just one of the available binding.

### 2. Configuration of the AWS credentials

The access key ID and secret access key as well as the endPoint and region and the Client configurations like the maxErrorRetry can be set in a properties file under s3-binding/conf/s3.properties or sent by command line (see below).
It is highly suggested to use the property file instead of to send the credentials through the command line.
    

### 3. Run YCSB

To execute the benchmark using the S3 storage binding, first files must be uploaded using the "load" option with this command:

    ./bin/ycsb load s3 -p table=theBucket -p s3.endPoint=s3.amazonaws.com -p s3.accessKeyId=yourAccessKeyId -p s3.secretKey=yourSecretKey -p recordcount=10 -p operationcount=10 -p workload=com.yahoo.ycsb.workloads.CoreWorkload -p readallfields=true -p readproportion=0.5 -p updateproportion=0.5 -p scanproportion=0 -p insertproportion=0 -p readmodifywriteproportion=0  -p fieldlength=10 -p fieldcount=20 -threads 10 -p requestdistribution=zipfian

With this command, 10 files will be uploaded. The file size is determined by the number of fields (fieldcount) and by the field size (fieldlength). In this case each file is 200 bytes (10 bytes for each field multiplied by 20 fields). With this command 10 threads will be used.

Running the command:

       ./bin/ycsb load s3 -p table=theBucket -p s3.endPoint=s3.amazonaws.com -p s3.accessKeyId=yourAccessKeyId -p s3.secretKey=yourSecretKey -p recordcount=10 -p operationcount=10 -p workload=com.yahoo.ycsb.workloads.CoreWorkload -p readallfields=true -p readproportion=0.5 -p updateproportion=0.5 -p scanproportion=0 -p insertproportion=0 -p readmodifywriteproportion=0  -p fieldlength=10 -p fieldcount=20 -threads 10 -p requestdistribution=zipfian

the workload A will be executed with 50/50 updates/reads. 

#### S3 Storage Configuration Parameters
- `table`
  - This should be a S3 Storage bucket name and it replace the standard table name assigned by YCSB. 
 
- `s3.endpoint`
  - This indicate the endpoint used to connect to the S3 Storage service.
  - Default value is `s3.amazonaws.com`.

- `s3.region`
  - This indicate the region where your buckets are.
  - Default value is `us-east-1`.
 
- `s3.accessKeyId`
  - This is the accessKey of your S3 account.
 
- `s3.secretKey`
  - This is the secret associated with your S3 account.

- `s3.maxErrorRetry`
  - This is the maxErrorRetry parameter for the S3Client.
