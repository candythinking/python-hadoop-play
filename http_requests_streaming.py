import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print >> sys.stderr, "Usage: http_requests_streaming <broker> <topic>"
        exit(-1)
    
    broker = sys.argv[1]
    topic = sys.argv[2]  
    
    sc = SparkContext()

    # Configure the Streaming Context with a 1 second batch duration
    ssc = StreamingContext(sc,5)

    # Creat DStream using KafkaUtils
    kafka_stream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": broker})
    
    # Test
    kafka_stream.pprint(1)

    # End of the application
    ssc.start()
    ssc.awaitTermination()
    sc.stop()