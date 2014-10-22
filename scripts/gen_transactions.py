from random import gauss
from datetime import timedelta
from random import randint
from datetime import *
from math import fabs
from kafka import KafkaClient, SimpleProducer, SimpleConsumer
import time
import random
import uuid
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

iterations = int(sys.argv[1])
logger = logging.getLogger(__name__)
#formatter = MyFormatter(fmt='%(asctime)s %(message)s',datefmt='%Y-%m-%d-%H:%M:%S.%f')
formatter = MyFormatter(fmt='%(asctime)s %(message)s')

logger.setLevel(logging.DEBUG)
#
fh = logging.FileHandler('logs/generated_txn.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
#
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(formatter)
#
logger.addHandler(console)
logger.addHandler(fh)

# To send messages synchronously
kafka = KafkaClient("ip-10-218-130-139.ec2.internal:9092")
producer = SimpleProducer(kafka)
topic = "flume.txn"


clines = open('customers_final.txt').read().splitlines()
slines = open('stores_final.txt').read().splitlines()

logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
for c_num in range(1,iterations):
    #print ("iteration " + str(c_num))
    random_customer_line=random.choice(clines)
    random_store_line =random.choice(slines)
    rand_amount= str(fabs(round(gauss(40,40),2)));
    c_fields = random_customer_line.split('|')
    s_fields = random_store_line.split('|')
    txn_id = str(uuid.uuid1())
    txn_line = txn_id + "|" + c_fields[0] + "|" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "|" + rand_amount + "|" + s_fields[0] 
    logger.debug(txn_id)
    print (txn_line)
    producer.send_messages(topic, txn_line.encode('utf8'));
#    time.sleep(1)

logger.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
kafka.close()
