Fraud Demo
==========
This demo is an exaple of integrating Apche Kafka with Hadoop / Apache HBase using Apache Flume. It utilizes the Flume-Kafka source and sink, available in CDH 5.2/Flume 1.5.0. This demo covers the scenario beyond basic ingestion of events, and performs some near-real-time processing of the data to return a response during event ingest.

To demonstrate this capability, we'll use a ficticious scenario of procssing debit card transactions for Fraud. A common problem in card processing  is identifying the likelihood that a customer is traveling. A debit card customer doesn't want to have their card declined if they are legitimately traveling, yet also want their card protected in the case of true fraud. This scenario utilizes a very simple travel scoring calculation, essentially determinig if it is physically possible for the customer to be at another location given the time window since the last transaction. This logic acts as a substitute to more advanced detection algorithms; for example, examing the series of last transactions to determine if a recent previous transaction was conducted at or near an airport, or if a recent purchase was for airfare.

The demo is built using CDH 5.2 and requires Flume and HBase to be configured. In the example I share Zookeeper across all services but this definitely is not recommended for anything other than examples. Additionally, in the code there are some shortcuts, additional logging / debugging that are added for visibility. One example is using a basic string messsage type rather than serializing the events themselves properly using Avro. 

Below are detailed steps and configuration in order to setup and run this example. Cloudera Manager(CM) is used. 

##Setup

The project can be built with 

    mvn clean compile package

1) Install CDH 5.2 with ZK, Flume and HBase.<br> 
2) Install Kafka via the documentation here: http://www.cloudera.com/content/cloudera/en/developers/home/cloudera-labs/apache-kafka.html 

The default location for custom service descriptors (CSD)s is /opt/cloudera/csd. After downloading CLABS_KAFKA-1.0.0.jar from Cloudera, copy the file to /opt/cloudera/csd/ in the Cloudera Manager server.

Restart the Cloudera Management Services and then download, distribute and activate the parcel. 
Then from the CM home screen, Add Service->Kafka
Follow the prompts.

3) Create the kafka topics:
    
    bin/kafka-topics --zookeeper zk-host:port/kafka --create --topic flume.txn --replication-factor 1 --partitions 1
    bin/kafka-topics --zookeeper zk-host:port/kafka --create --topic flume.auths --replication-factor 1 --partitions 1
_*Note the need to specify the ZK root (default kafka) after the port in the --zookeeper option_

4) Copy the fraud-demo.jar to /opt/cloudera/parcels/CDH/lib/flume-ng/lib/ on the Flume agent host<br>
5) Copy the sample flume configuration and modify with appropriate values 
cp fraud.demo.jar /opt/cloudera/parcels/CDH/lib/flume-ng/lib/

6) To setup the HBase tables and seed the data need for the setup change to the scripts/ directory and run LoadSeedData with the option of customer | store | both eg:

    java -cp ../target/fraud.demo.jar:`hbase classpath` cloudera.se.fraud.demo.util.LoadSeedData both

7) To generate transactions using the provided scripts:
  1. yum install python-pip
  2. pip install kafka-python
  3. mkdir scripts/logs
  4. Modify the broker and topic in gen_transactions.py  
  5. Execute gen_transactions.py #of transactions eg:
  
```python 
gen_transactions.py 100
```


8) To consume messages
  1. Modify the broker and topic in read_kafka.py
  2. Start the consumer:

```python 
read_transactions.py 
```

All of the debug / log messages for the Interceptor will be in the /var/log/flume-ng/interceptor.log file
Logging for the python consumer/producers will ge in the logs/ directory you created before.


##Code Walkthrough

customer_final.txt contains a list of customers in the following format

Credit Card Number | Name | Home Lat | Home Lon
-------------------|------|----------|---------
4539390409269|Solomon, Leila Z.|33.58890545|-84.9569793
4556899046304500|Wolfe, Minerva J.|33.30795222|-83.97710263

stores_final.txt:

Store ID | Store Name | Address | Lat | Lon | Phone Number | Type | MCC
---------|------------|---------|-----|-----|--------------|------|-----
1|AGILEST COLLECTIONS|127 E TRINITY PLDECATUR GA 30030|33.773344|-84.296021||FURNISHINGS / APPLIANCES OFFICE & HOME |5712
2|AGNES SCOTT COLLEGE|141 E COLLEGE AVEDECATUR GA 30030|33.76825|-84.294489|(404) 471-6000|BUSINESS / PROFESSIONAL SERVICES |7399

The LoadSeedData method in the java application simply loads these files into HBase, for retrieval later. 
The gen_transaction.py script picks a random record from the stores and customers table and generates a transaction:

```
UID|CC #|Transaction Date|Amount|StoreID
31f0dcc8-5f2a-11e4-8c2f-06902e00013f|4556899046304500|2014-10-28 22:12:35|15.39|543
31f16d6e-5f2a-11e4-8c2f-06902e00013f|4024007177047|2014-10-28 22:12:35|60.29|1083
```

### Java Application
####Project Structure

<ul>
<li>cloudera.se.fraud.demo</li>
  <ul> 
       <li>flume.interceptor</li>
        <ul>
            <li>FraudEventInterceptor</li>
       </ul>
       <li>model</li>
       <ul>
         <li>CustomerPOJO</li>
         <li>DataModelConstants</li>
         <li>FinalTransactionsPOJO</li>
         <li>StorePOJO</li>
         <li>TravelResultPOJO</li>
         <li>TravelScorePOJO</li>
        </ul>       
          <li>service</li>
           <ul>
             <li>HbaseFraudService</li>
             <li>TravelScoreService</li>
           </ul>
      
           <li>util</li>
             <ul> 
                <li>LoadSeedData</li>
              </ul>
            
   
</ul>




The customer table is modeled after com.cloudera.se.demo.fraud.model.CustomerPOJO

```java
public class CustomerPOJO {

    long customerId;
    String name;
    String homeLat;
    String homeLon;
    String last20Locations;
    String last20Amounts;
    double lastTransactionAmount;
    String lastTransactionLat;
    String lastTransactionLon;
    String lastTransactionTime;
    double totalSpent;
    double avgSpent;
    int transactionCount;
    String rowKey;
```
We store the last transaction values as well as some counters, like totalSpent, avgSpent and transactionCount. The last20 columns collect the previous 20 values for a customer. This allows for easy lookups when enriching the transaction.

The project implements an Interceptor to process the events as they are read from Kafka. 
The interceptor does the following:

1)	Gets the extended customer information from HBase
2)	Gets the extended store information from HBase
3)	Calculates the Travel Score
4)	Puts some updated values back to HBase

It also accepts a threadNum parameter which controls the number of threads spawned to process our events. The default value is 5.

We implement our application logic in the method 

```java 
public Event intercept(Event event)
```
From our transaction before, we’re just using a simple text message, so we parse out some details from the message in order to call Hbase:
```java
Pattern p = Pattern.compile("\\|+");
String txn = Bytes.toString(event.getBody());
String[] tokens = p.split(txn);

String txnId = tokens[0];
long customerId = Long.parseLong(tokens[1]);
String txnTime = tokens[2];
double txnAmount = Double.parseDouble(tokens[3]);
int merchantId = Integer.parseInt(tokens[4]);
String txnLat = null;
String txnLon = null;

CustomerPOJO customer = null;
StorePOJO store = null;
```

We’re now setup to call our HBase service, which uses the customerId and storeId to populate a Customer and Store POJO respectively.

```java
try {
     log.debug("Getting Customer " + customerId);
     customer = hbaseFraudService.getCustomerFromHBase(customerId, txnId);
     log.debug("Getting Store " + merchantId);
     store = hbaseFraudService.getStoreFromHBase(merchantId, txnId);
     String[] location = store.getLocation().split("\\,");
     txnLat = location[0];
     txnLon = location[1];
     } catch (IOException e) {
            log.debug(e);
    }
```
The store table contains lat/long data about the store and the customer table stores the last transaction location and timestamp for a particular customer. From this information we can do a simple calculation to determine if it’s physically possible to have made the transaction in the given timeframe.

```java
score.setLocation1(nvl(customer.getLastTransactionLat(), customer.getHomeLat()).concat(",").concat(nvl(customer.getLastTransactionLon(), customer.getHomeLon())));
score.setLocation2(store.getLocation());
score.setTime1(customer.getLastTransactionTime());
score.setTime2(txnTime);
        try {
            result = travelScoreService.calcTravelScore(score);
        } catch (Exception e) {
            log.debug(e);
        }
```
After calculating the travel score, we now have everything we need for the actual event. So we create a new record via constructor:
```java
FinalTransactionPOJO finalTxn = new FinalTransactionPOJO(fields…)
```
We will now update some basic data in our Customers table so that the next run has the latest values that we might need in our event processing. 
```java
customer.setLastTransactionAmount(txnAmount);
customer.setLastTransactionLat(txnLat);
customer.setLastTransactionLon(txnLon);
customer.setLastTransactionTime(txnTime);
        customer.setLast20Amounts(HbaseFraudService.pushValue(String.valueOf(txnAmount), customer.getLast20Amounts()));
customer.setLast20Locations(HbaseFraudService.pushValue(txnLat +"|"+ txnLon, customer.getLast20Locations()));
```java

With the customer record updated, we can persist these values back to HBase:

```java
try { log.debug("Saving Customer");
      hbaseFraudService.saveCustomerToHbase(customer, txnId); 
    }
     catch (IOException e) {
       log.debug(e);
     }
```java
For final formatting, we’ll convert to JSON and return the event:

```java
String message = convertToJSON(finalTxn);
event.setBody(message.getBytes());
log.debug("returning event");
return event;
```java

Because flume processes events in batches, we must also implement the method below to process the entire batch, which will call our intercept(event) method. Below is the standard implementation:

```java
public List<Event> intercept(List<Event> events) {
for (Event event : events) {
      intercept(event);
    }
    return events;
  }
```
In order to process each event as quickly as possible, we’ll use multiple threads to process the batch in parallel, as HBase can scale to accommodate multiple threads per region server.

```java
public List<Event> intercept(List<Event> events) {
        log.info("Starting Interceptor Batch");
        
        ArrayList<Callable<Event>> callableList = new ArrayList<Callable<Event>>();
        for (final Event event : events) {
            callableList.add(new Callable<Event>() {
                @Override
                public Event call() {
                    intercept(event);
                    return event;
                }
            });
        }
        try {
            List<Future<Event>> futures = executorService.invokeAll(callableList);
        } catch (InterruptedException e) {
            log.debug(e);
        }
        log.info("Ending Interceptor Batch");
        return events;
    }
```
 


