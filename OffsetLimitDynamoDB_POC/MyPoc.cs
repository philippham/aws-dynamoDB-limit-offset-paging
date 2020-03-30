using Amazon.DynamoDBv2.DataModel;

namespace OffsetLimitDynamoDB_POC
{
    public class MyPoc
    {
        [DynamoDBHashKey]
        public string PartitionKey { get; set; }

        [DynamoDBRangeKey]
        public string SortKey { get; set; }
        [DynamoDBProperty]
        public string ReferenceId { get; set; }
    }
}
