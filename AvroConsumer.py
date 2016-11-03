import json,io,avro.schema,avro.io,sys
from kafka import KafkaConsumer
from pykafka import KafkaClient

"""AvroConsumer.py broker_host topic_name schema_path num
   AvroConsumer.py broker_host topic_name schema_path num --from-beginning"""
def getSchema(path):
    with open(path) as schemaFile:
        schema = avro.schema.parse(schemaFile.read())
        return schema
def AvroToJson(record, schema):
    bytes_reader = io.BytesIO(record)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    jsonRecord = reader.read(decoder)
    return jsonRecord
def Consumer(broker_host,topic_name,schema,num):
    consumer=KafkaConsumer(topic_name,bootstrap_servers=broker_host,value_deserializer=lambda m:AvroToJson(m,schema))
    n=0
    for message in consumer:
        print json.dumps(message.value,indent=4)
        n+=1
        if n==int(num):break
def ConsumerFB(broker_host,topic_name,schema,num):
    client=KafkaClient(broker_host)
    topic=client.topics[topic_name]
    consumer=topic.get_simple_consumer(reset_offset_on_start=True)
    n = 0
    for message in consumer:
        m=AvroToJson(message.value,schema)
        print json.dumps(m,indent=4)
        n += 1
        if n == int(num):break
def main():
    if len(sys.argv)==5:
        schema = getSchema(sys.argv[3])
        broker_host=sys.argv[1]
        topic_name=sys.argv[2]
        num=sys.argv[4]
        Consumer(broker_host,topic_name,schema,num)
    elif len(sys.argv)==6 and sys.argv[5]=="--from-beginning":
        schema = getSchema(sys.argv[3])
        broker_host = sys.argv[1]
        topic_name = sys.argv[2]
        num = sys.argv[4]
        ConsumerFB(broker_host,topic_name,schema,num)
    else:
        print "Usage:\n" \
              " python2.7 AvroConsumer.py broker_host:6667 topic_name schema_path num\n" \
              " python2.7 AvroConsumer.py broker_host:6667 topic_name schema_path num --from-beginning\n"
if __name__=="__main__":
    main()
