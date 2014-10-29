Fraud Demo
==========
This demo is an exaple of integrating Apche Kafka with Hadoop / Apache HBase using Apache Flume. It utilizes the Flume-Kafka source and sink, available in CDH 5.2/Flume 1.5.0. This demo covers the scenario beyond basic ingestion of events, and performs some near-real-time processing of the data to return a response during event ingest.

To demonstrate this capability, we'll use a ficticious scenario of procssing debit card transactions for Fraud. A common problem in card processing  is identifying the likelihood that a customer is traveling. A debit card customer doesn't want to have their card declined if they are legitimately traveling, yet also want their card protected in the case of true fraud. This scenario utilizes a very simple travel scoring calculation, essentially determinig if it is physically possible for the customer to be at another location given the time window since the last transaction. This logic acts as a substitute to more advanced detection algorithms; for example, examing the series of last transactions to determine if a recent previous transaction was conducted at or near an airport, or if a recent purchase was for airfare.

The demo is built using CDH 5.2 and requires Flume and HBase to be configured. In the example I share Zookeeper across all services but this definitely is not recommended for anything other than examples. Additionally, in the code there are some shortcuts, additional logging / debugging that are added for visibility. One example is using a basic string messsage type rather than serializing the events themselves properly using Avro. 

Below are detailed steps and configuration in order to setup and run this example. Cloudera Manager(CM) is used. 

#Setup

The project can be built with 

    mvn clean compile package

1) Install CDH 5.2 with ZK, Flume and HBase. 
2) Install Kafka via the documentation here: http://www.cloudera.com/content/cloudera/en/developers/home/cloudera-labs/apache-kafka.html 

The default location for custom service descriptors (CSD)s is /opt/cloudera/csd. After downloading CLABS_KAFKA-1.0.0.jar from Cloudera, copy the file to /opt/cloudera/csd/ in the Cloudera Manager server.

Restart the Cloudera Management Services and then download, distribute and activate the parcel. 
Then from the CM home screen, Add Service->Kafka
Follow the prompts.

3) Create the kafka topics:
    
    bin/kafka-topics --zookeeper zk-host:port/kafka --create --topic flume.txn --replication-factor 1 --partitions 1
    bin/kafka-topics --zookeeper zk-host:port/kafka --create --topic flume.auths --replication-factor 1 --partitions 1
*Note the need to specify the ZK root (default kafka) after the port in the --zookeeper option

4) Copy the fraud-demo.jar to /opt/cloudera/parcels/CDH/lib/flume-ng/lib/ on the Flume agent host
5) Copy the sample flume configuration and modify with appropriate values 
cp fraud.demo.jar /opt/cloudera/parcels/CDH/lib/flume-ng/lib/

6) To setup the HBase tables and seed the data need for the setup change to the scripts/ directory and run LoadSeedData with the option of customer | store | both eg:

    java -cp ../target/fraud.demo.jar:`hbase classpath` cloudera.se.fraud.demo.util.LoadSeedData both

8) To generate transactions using the provided scripts:
  1. yum install python-pip
  2. pip install kafka-python
  3. mkdir scripts/logs
  4. Modify the broker and topic in gen_transactions.py
  5. execute gen_transactions.py #of transactions eg:
  
       python gen_transactions.py 100

9) To consume messages
  1) Modify the broker and topic in read_kafka.py
  2) Start the consumer:
  
      python read_transactions.py


All of the debug / log messages for the Interceptor will be in the /var/log/flume-ng/interceptor.log file
Logging for the python consumer/producers will ge in the logs/ directory you created before.


#Code Walkthrough




 


