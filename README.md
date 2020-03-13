# PythonKafkaSubprocess

<100-line KafkaConsumer written in python, designed as a Node.js child_process.spawn() multi-threadable script.

The existing node.js Kafka bindings were not working... So I wrote a port to the Python Kafka consumer, which works much better and is multi-threadable. Node.js can read consumes using stdio.

This is configured to read topics from the beginning, and gets the High Watermark to ensure completion of reads.

# Run the script

Run the script using 
> `node nodeMasterProcess.js`

You may need to run `pip install kafka-python` to download dependencies.

This code only contains Kafka Consumer code. Would anybody like to see a Producer thread added to it?
