using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace OffsetLimitDynamoDB_POC
{
    class Program
    {
        static string PK = "renaissance:tenant:ppham";
        static string SK = "enrollment::";
        static string TABLE = "global-test-Rostering-v1";
        static int LIMIT = 2;

        static async Task Main(string[] args)
        {
            var allEnrollments = await GetAll();
            Print(allEnrollments, -1);

            int offset = 0;
            bool hasMore = true;
            while (hasMore)
            {
                var enrollment = await GetWithOffsetAndLimit(offset, LIMIT);
                if (enrollment == null || enrollment.Count < LIMIT)
                {
                    hasMore = false;
                }
                else
                {
                    Print(enrollment, offset);
                    offset += LIMIT;
                }
            }
        }

        private static void Print(List<MyPoc> enrollment, int offset)
        {
            if (offset == -1)
            {
                Console.WriteLine("All enrollments:");
            }
            else
            {
                Console.WriteLine($"Result for Offset {offset}:");
            }
            foreach (var item in enrollment)
            {
                Console.WriteLine($"{item.PartitionKey}-{item.SortKey}");
            }
            Console.WriteLine();
        }

        private static DynamoDBContext GetContext()
        {
            AWSCredentials credentials = new StoredProfileAWSCredentials("rli-dev", "C:/Users/pnpham/.aws/credentials");
            var Client = new AmazonDynamoDBClient(credentials, Amazon.RegionEndpoint.USEast1);
            var contextConfig = new DynamoDBContextConfig();

            return new DynamoDBContext(Client, contextConfig);
        }

        private async static Task<List<MyPoc>> GetWithOffsetAndLimit(int offSet, int limit)
        {
            if (offSet != 0)
            {
                TrackingOffset tracking = await GetTracking(offSet, limit);
                if (tracking == null)
                {
                    Console.WriteLine("Please start from Offset zero.");
                    return null;
                }

                // Set tracking.Expiration = some config seconds

                //tracking.Expiration is valid ??

                var enrollments = await GetWithLastKey(tracking.LastKey);

                if (enrollments.Count >= LIMIT)
                {
                    await SaveTracking(offSet, limit, enrollments[LIMIT - 1].SortKey);
                    return enrollments.Take(LIMIT).ToList();
                }
                else
                    return enrollments;
            }
            else
            {
                var enrollments = await GetWithLimit(LIMIT);

                if (enrollments.Count >= LIMIT)
                {
                    await SaveTracking(offSet, limit, enrollments[LIMIT - 1].SortKey);
                    return enrollments.Take(LIMIT).ToList();
                }
                else
                    return enrollments;
            }
        }

        private static async Task<TrackingOffset> GetTracking(int offSet, int limit)
        {
            // Get TrackingOffset
            var context = GetContext();
            var tracking = await context.LoadAsync<TrackingOffset>(hashKey: $"ppham_{offSet - limit}_{limit}", rangeKey: $"ppham_{offSet - limit}_{limit}", new DynamoDBOperationConfig()
            {
                OverrideTableName = TABLE
            });
            return tracking;
        }

        private static async Task SaveTracking(int offSet, int limit, string lastKey)
        {
            var context = GetContext();
            var tracking = new TrackingOffset { PartitionKey = $"ppham_{offSet}_{limit}", SortKey = $"ppham_{offSet}_{limit}", LastKey = lastKey };
            await context.SaveAsync(tracking, new DynamoDBOperationConfig()
            {
                OverrideTableName = TABLE
            });
        }

        private async static Task<List<MyPoc>> GetWithLimit(int limit)
        {
            var context = GetContext();

            var asyncSearch = context.QueryAsync<MyPoc>(PK, QueryOperator.BeginsWith, new List<string>() { SK }, new DynamoDBOperationConfig()
            {
                OverrideTableName = TABLE
            });

            var enrollments = new List<MyPoc>();
            while (!asyncSearch.IsDone)
            {
                enrollments.AddRange(await asyncSearch.GetNextSetAsync());
                if (enrollments.Count >= limit)
                {
                    return enrollments;
                }
            }

            enrollments.AddRange(await asyncSearch.GetRemainingAsync());

            return enrollments;
        }

        private async static Task<List<MyPoc>> GetAll()
        {
            var context = GetContext();

            var asyncSearch = context.QueryAsync<MyPoc>(PK, QueryOperator.BeginsWith, new List<string>() { SK }, new DynamoDBOperationConfig()
            {
                OverrideTableName = TABLE
            });

            var enrollments = new List<MyPoc>();
            while (!asyncSearch.IsDone)
            {
                enrollments.AddRange(await asyncSearch.GetNextSetAsync());
            }

            enrollments.AddRange(await asyncSearch.GetRemainingAsync());

            return enrollments;
        }

        private async static Task<List<MyPoc>> GetWithLastKey(string lastKey)
        {
            var context = GetContext();

            var asyncSearch = context.QueryAsync<MyPoc>(PK, QueryOperator.GreaterThan, new List<string>() { lastKey }, new DynamoDBOperationConfig()
            {
                OverrideTableName = TABLE
            });

            var enrollments = new List<MyPoc>();

            while (!asyncSearch.IsDone)
            {
                enrollments.AddRange(await asyncSearch.GetNextSetAsync());
            }

            enrollments.AddRange(await asyncSearch.GetRemainingAsync());

            return enrollments;
        }
    }
}
