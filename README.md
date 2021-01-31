# media-base-s3-notifier

A small AWS Lambda function which posts a JSON message to Azure ServiceBus on S3 object creation.


Lambda needs to provide these environmental variables:
* `AZURE_BUS_CONNECTION_STRING`
* `AZURE_BUS_TOPIC_NAME`
