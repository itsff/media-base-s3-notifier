namespace MediaBase
{
    using System;

    /// <summary>
    /// Message that will be sent on the bus to ingestor bot.
    /// </summary>
    public class FileAddedBusMessage
    {
        public DateTimeOffset TimeStamp { get; set; }
        public string AwsRegion { get; set; }
        public string S3Bucket { get; set; }
        public string S3ObjectKey { get; set; }
        public string ETag { get; set; }

        public string Id => $"{S3Bucket}::{S3ObjectKey}::{TimeStamp.ToUnixTimeMilliseconds()}";
    }
}