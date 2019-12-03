import threading                                                                                                                                                                                                                                                                                                                                                     
import multiprocessing
import sys
import string
import time
import random
import json
from kafka import KafkaConsumer
from kafka import SimpleClient
from kafka.protocol.offset import OffsetRequest

def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))
group_id = randomString(32);

# The delimiter is written between messages and used to splice together Buffer strings
# It is important the delimiter pattern is not expected inside the Kafka messages
delimiter = chr(3) + chr(0) + chr(4)

# These variables need to be thread-safe, as they are shared between threads
topicsPending = threading.Event()
subscribedTopics = []

class Consumer(threading.Thread):
    daemon = True
    def run(self):
       watermarked = {}
       consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
           group_id=group_id,
           auto_offset_reset='earliest',
           value_deserializer=lambda m: json.loads(m.decode('utf-8')))

       while True:
           if topicsPending.is_set():
               consumer.unsubscribe()
               consumer.subscribe(subscribedTopics)
               topicsPending.clear()

           messages = consumer.poll(timeout_ms=500);
           payload = ''
           for message in  messages.items():
               for item in message[1]:
                       contents = json.dumps(item.value)
                       payload += '{"topic": "%s", "offset": %s, "value": %s}' % (item.topic, item.offset, contents)
                       payload += delimiter

           if payload:
               print payload
               sys.stdout.flush()


           payload = ''
           for message in messages.items():
               topic = message[0][0];
               if not topic in watermarked:
                   # This is a probably a blocking call... may be better if it were separately threaded?
                   highwater = consumer.highwater(message[0])
                   watermarked[topic] = True
                   payload += '{"topic": "%s", "highwater": %s}' % (topic, highwater)
                   payload += delimiter;

           if payload:
               print payload
               sys.stdout.flush()


class IO(threading.Thread):
    daemon = True
    def run(self):
        for line in iter(sys.stdin.readline, ''):
            message = json.loads(line);
            if message['command'] == 'readTopic':
                subscribedTopics.append(message['topic']);
                topicsPending.set();
            else:
                print >> sys.stderr, 'Python - Invalid message %s' % line


def main():
    threads = [
        Consumer(),
        IO(),
    ]

    for t in threads:
        t.start()

    # These last one hour
    time.sleep(60 * 60)

if __name__ == "__main__":
    main()
