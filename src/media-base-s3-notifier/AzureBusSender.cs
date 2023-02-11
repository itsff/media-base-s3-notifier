namespace MediaBase
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Amazon.Lambda.Core;
    using Microsoft.Azure.ServiceBus;
    using Microsoft.Azure.ServiceBus.Core;

    public class AzureBusSender : IBusSender
    {
        readonly MessageSender _client;
        
        public static AzureBusSender CreateFromEnv ()
        {
            var result = new AzureBusSender(
                Environment.GetEnvironmentVariable("AZURE_BUS_CONNECTION_STRING"),
                Environment.GetEnvironmentVariable("AZURE_BUS_TOPIC_NAME")
            );

            return result;
        }

        public AzureBusSender (string connectionString, string topicName)
        {
            _client = new MessageSender(connectionString, topicName);
        }


        public Task SendBatch(IEnumerable<FileAddedBusMessage> msgs, ILambdaLogger logger)
        {
            logger.LogLine("Preparing Azure Bus messages");
            return _client.SendAsync(SerializeMessages(msgs, logger));
        }

        protected IList<Message> SerializeMessages (IEnumerable<FileAddedBusMessage> msgs, ILambdaLogger logger)
        {
            var result = new List<Message>();

            foreach (var m in msgs)
            {
                var bytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(m);

                var byteMsg = new Message(bytes);
                byteMsg.ContentType = "application/json";
                byteMsg.MessageId = m.Id;
                byteMsg.Label = "S3 File";
                result.Add(byteMsg);

                logger.LogLine($"serialized: {byteMsg.MessageId}");
            }
            return result;
        }
    }
}