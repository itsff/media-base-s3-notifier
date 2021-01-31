using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Util;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace MediaBase
{
    public class S3Notifier
    {
        readonly IAmazonS3 _S3Client;
        readonly IBusSender _busSender;

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public S3Notifier() : this(new AmazonS3Client(), AzureBusSender.CreateFromEnv())
        {
        }

        /// <summary>
        /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
        /// </summary>
        /// <param name="s3Client"></param>
        public S3Notifier(IAmazonS3 s3Client, IBusSender busSender)
        {
            _S3Client = s3Client;
            _busSender = busSender;
        }
        
        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
        /// to respond to S3 notifications.
        /// </summary>
        /// <param name="evnt"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<string> FunctionHandler(S3Event ev, ILambdaContext context)
        {
            try
            {
                IList<FileAddedBusMessage> messages = PrepareMessages(ev);
                if (messages.Count > 0)
                {
                    await _busSender.SendBatch(messages, context.Logger);
                    context.Logger.LogLine($"Sent {messages.Count} messages to bus");
                    return $"Num queued messages: {messages.Count}"; 
                }
                else
                {
                    return "No S3 messages to queue up";
                }
            }
            catch(Exception e)
            {
                context.Logger.LogLine("Problem queing up bus messages!");
                context.Logger.LogLine(e.Message);
                context.Logger.LogLine(e.StackTrace);
                throw;
            }
        }

        private IList<FileAddedBusMessage> PrepareMessages (S3Event ev)
        {
            var msgs = new List<FileAddedBusMessage>();

            if (ev.Records != null)
            {
                foreach(var record in ev.Records)
                {
                    if (record.S3 != null)
                    {
                        var m = new FileAddedBusMessage {
                            TimeStamp = record.EventTime,
                            S3Bucket = record.S3.Bucket.Name,
                            S3ObjectKey = record.S3.Object.Key,
                            ETag = record.S3.Object.ETag,
                            AwsRegion = record.AwsRegion
                        };
                        msgs.Add(m);
                    }
                }
            }

            return msgs;
        }
    }
}
