# Amazon Kinesis spark streaming source

Description
-----------
Spark streaming source that reads from AWS Kinesis streams.

Use Case
--------
This source is used when you want to read data from a Kinesis stream in real-time. For example, you may want to read
data from a Kinesis stream, apply transformations and write transformed data to a sink.

Properties
----------
**Application Name:** The name of the Kinesis application. The application name that is used to checkpoint the Kinesis sequence
numbers in DynamoDB table. (Macro-enabled)

**Stream Name:** The name of the Kinesis stream to the get the data from. The stream should be active. (Macro-enabled)

**Kinesis Endpoint Url:** Valid Kinesis endpoint URL eg. <protocol>://kinesis.us-east-1.amazonaws.com. For more
 information on endpoint URL please look at https://docs.aws.amazon.com/general/latest/gr/ak.html (Macro-enabled)

**Region:** Valid Kinesis region URL eg. ap-south-1. Default value is us-east-1.

**Checkpoint Interval Duration:** The interval in milliseconds at which the Kinesis Client Library saves its position in the stream.(Macro-enabled)

**Initial Position In Stream:** Initial position in the stream. Can be either TRIM_HORIZON or LATEST, Default
 position will be Latest.
 
**AWS Access Key Id:** The access Id provided by AWS required to access the Kinesis streams. The Id can be stored in
 secure store and can be provided as macro configuration. (Macro-enabled)

**AWS Access Secret:** AWS access key secret having access to Kinesis streams. The key can be stored in secure store and
can be provided as macro configuration. (Macro-enabled)

**Format:** Optional format of the Kinesis shard payload. For example, a value of 'csv' will attempt to parse Kinesis
 payloads as comma-separated values. If no format is given, Kinesis payloads will be treated as bytes.

Example
-------
This example will read from Kinesis stream named 'MyKinesisStream'. It will spin up 1 Kinesis Receiver per shard for the
given stream. It starts pulling from the last checkpointed sequence number of the given stream.

    {
        "name": "KinesisSource",
        "type": "streamingsource",
        "properties": {
            "appName": "myKinesisApp",
            "streamName": "MyKinesisStream",
            "accessID": "my_aws_access_key",
            "accessKey": "my_aws_access_secret",
            "endpointUrl": "1",
            "duration": "2000",
            "initialPosition": "Latest",
            "format": "csv",
            "schema": "{
                    \"type\":\"record\",
                    \"name\":\"purchase\",
                    \"fields\":[
                        {\"name\":\"user\",\"type\":\"string\"},
                        {\"name\":\"item\",\"type\":\"string\"},
                        {\"name\":\"count\",\"type\":\"int\"},
                        {\"name\":\"price\",\"type\":\"double\"}
                    ]
            }
        }
    }

For each Kinesis message read, it will output a record with the schema:

    +================================+
    | field name  | type             |
    +================================+
    | user        | string           |
    | item        | string           |
    | count       | int              |
    | price       | double           |
    +================================+
