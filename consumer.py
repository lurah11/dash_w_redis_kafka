import json 
from confluent_kafka import Consumer, KafkaException
from producer import topic
import redis

conf = {
    'bootstrap.servers':'localhost:9092',
    'group.id':'tampan',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(conf)

redis_client = redis.StrictRedis(decode_responses=True)

def basic_consume_loop(consumer,topics=[topic]): 
    running = True
    try: 
        consumer.subscribe(topics)
        timeout_count = 300
        while running : 
            if timeout_count == 0 : 
                break
            msg = consumer.poll(timeout=1.0)
            if msg is None: 
                timeout_count -= 1
                print(f"waiting for data.... timeout in {timeout_count} s")
                continue 
            if msg.error(): 
                raise(KafkaException(msg.error()))
            else : 
                data = json.loads(msg.value().decode('utf-8'))
                redis_client.zadd("item_order",{str(data['index']):data['index']})
                redis_client.set(str(data['index']),msg.value())
                print(data)
             
    finally:
        running = False
        consumer.close()    

basic_consume_loop(consumer)


