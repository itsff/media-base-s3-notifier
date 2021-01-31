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
        readonly ILambdaSerializer _serializer;
        readonly byte[] _buffer;
        
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
            _serializer = new Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer();
            _buffer = new byte[512];
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
                Array.Fill(_buffer, (byte)0);
                using (var stream = new System.IO.MemoryStream(_buffer))
                {
                    _serializer.Serialize(m, stream);
                    stream.Flush();
                    stream.Close();

                    var byteMsg = new Message(stream.ToArray());
                    byteMsg.ContentType = "application/json";
                    byteMsg.MessageId = m.Id;
                    byteMsg.Label = "S3 File";
                    result.Add(byteMsg);

                    logger.LogLine($"serialized: {byteMsg.MessageId}");
                }
            }
            return result;
        }
    }
}