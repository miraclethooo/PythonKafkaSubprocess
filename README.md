# PythonKafkaSubprocess

<100-line KafkaConsumer written in python, designed as a Node.js child_process.spawn multi-threadable script.

This reads topics from the beginning, and gets the High Watermark to ensure completion of reads.

Note that a delimiter is used to notify Node.js of breaks between messages, since the Buffer stream that comes out on the Node.js side are concatenated together.
