namespace MediaBase
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Amazon.Lambda.Core;

    public interface IBusSender
    {
        Task SendBatch (IEnumerable<FileAddedBusMessage> msgs, ILambdaLogger logger);
    }
}