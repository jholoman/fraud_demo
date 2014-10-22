from kafka import KafkaClient, SimpleProducer, SimpleConsumer

# To send messages synchronously
import logging
import datetime as dt
import sys

class MyFormatter(logging.Formatter):
    converter=dt.datetime.fromtimestamp
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s.%04d" % (t, record.msecs)
        return s

logger = logging.getLogger(__name__)
#formatter = MyFormatter(fmt='%(asctime)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S.%f')
formatter = MyFormatter(fmt='%(asctime)s %(message)s')
logger.setLevel(logging.DEBUG)
#
fh = logging.FileHandler('logs/processed_txn.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
#
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(formatter)
#
logger.addHandler(console)
logger.addHandler(fh)

kafka = KafkaClient("ip-10-218-130-139.ec2.internal:9092")
consumer = SimpleConsumer(kafka, "flume-consumer", "flume.auths")
consumer.seek(0, 2)
for message in consumer:
    # message is raw byte string -- decode if necessary!
    # e.g., for unicode: `message.decode('utf-8')`
   #print(message)

   line =  message.message.value.split(",")
   print line
   logger.debug(message.message.value)
   print (line[0] + "|" + line[1])
   #print (line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15]) 
kafka.close()
