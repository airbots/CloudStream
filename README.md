# CloudStream
This is a sink for Kafka connecting to Openstack Swift Object Store. It is a kafka-connect based connector to copy data from Kafka to Openstack Swift Object Stores. It focusses on reliable and scalable data copying. It can write the data out in different formats (like parquet, so that it can readily be used by analytical tools) and also in different partitioning requirements.
## Features
CloudStream inherits from kafka-connect-hdfs. It has most of features that kafka-connect-hdfs has. It also borrowed some feature from Streamx which is a Qubole connector to work with S3, Google Cloud, and Azure.

## Tutorial 
CloudStream is based on kafka-connect-hdfs- 3.2. It means earlier version of confluent or kafka may have library issues when using CloudStream. 
### build CloudStream


### setup Classpath

### config CloudStream

### Run CloudStream in standalone mode

### Run CloudStream in distributed mode

### Docker

### Roadmap
