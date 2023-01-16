using Newtonsoft.Json.Linq;
using PowerApps.Samples;
using PowerApps.Samples.Batch;
using PowerApps.Samples.Messages;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace FKA.Dataverse.ConsoleApp
{
    internal class Program
    {
        // How many records to create and delete with this sample.
        static readonly int numberOfRecords = 10000;
        //static readonly int numberOfBuckets = numberOfRecords / 10;

        static async Task Main()
        {
            Config config = App.InitializeApp();

            var service = new Service(config);

            #region Optimize Connection

            // Change max connections from .NET to a remote service default: 2
            System.Net.ServicePointManager.DefaultConnectionLimit = 65000;
            // Bump up the min threads reserved for this app to ramp connections faster - minWorkerThreads defaults to 4, minIOCP defaults to 4 
            ThreadPool.SetMinThreads(100, 100);
            // Turn off the Expect 100 to continue message - 'true' will cause the caller to wait until it round-trip confirms a connection to the server 
            System.Net.ServicePointManager.Expect100Continue = false;
            // Can decrease overall transmission overhead but can cause delay in data packet arrival
            System.Net.ServicePointManager.UseNagleAlgorithm = false;

            #endregion Optimize Connection

            Console.WriteLine("--Starting Parallel Operations sample--");

            // Send a simple request to access the recommended degree of parallelism (DOP).
            HttpResponseMessage whoAmIResponse = await service.SendAsync(new WhoAmIRequest());
            int recommendedDegreeOfParallelism = int.Parse(whoAmIResponse.Headers.GetValues("x-ms-dop-hint").FirstOrDefault());

            Console.WriteLine($"Recommended degree of parallelism for this environment is {recommendedDegreeOfParallelism}.");

            var parallelOptions = new ParallelOptions() { MaxDegreeOfParallelism = recommendedDegreeOfParallelism };

            List<CreateRequest> itemsToImport = CreateRecords();

            Console.WriteLine($"\n\n\n****Creating {numberOfRecords} in single requests");
            await RunParallellSimpleTasks(service, parallelOptions, itemsToImport);

            int numberOfBuckets = numberOfRecords / 2;
            while (numberOfBuckets >= 1 && (numberOfRecords / numberOfBuckets) <= 999 )
            {
                Console.WriteLine($"\n\n\n****Creating {numberOfBuckets} buckets with ca {numberOfRecords / numberOfBuckets} records in each");
                var buckets = CreateBuckets(itemsToImport, numberOfBuckets);

                await RunParallellBatch(service, parallelOptions, buckets);
                numberOfBuckets = numberOfBuckets / 2;
            }



        }

        private static List<CreateRequest> CreateRecords()
        {
            var count = 0;

            // Will be populated with requests to create account records
            List<CreateRequest> itemsToImport = new();

            Console.WriteLine($"Preparing to create {numberOfRecords} items records using Web API.");

            // Add account create requests to accountsToImport,
            while (count < numberOfRecords)
            {
                var item = new JObject
                {
                    ["fka_unit"] = "Stk",
                    ["fka_source"] = "FKMeny",
                    ["fka_importid"] = $"T{count}",
                    ["fka_itemgroup"] = "A",
                    ["fka_productid"] = count.ToString(),
                    ["fka_productname"] = $"Item {count.ToString()}"
                };
                itemsToImport.Add(new CreateRequest("fka_productimportfkmenies", item));
                count++;
            }

            return itemsToImport;
        }

        private static async Task RunParallellSimpleTasks(Service service, ParallelOptions parallelOptions, List<CreateRequest> itemsToImport)
        {
            // ConcurrentBag is a thread-safe, unordered collection of objects.
            ConcurrentBag<DeleteRequest> itemsToDelete = new();

            try
            {
                //Console.WriteLine($"Creating {itemsToImport.Count} records");
                var startCreate = DateTime.Now;

                // Send the requests in parallel
                await Parallel.ForEachAsync(itemsToImport, parallelOptions, async (item, token) =>
                {
                    var createResponse = await service.SendAsync<CreateResponse>(item);

                    // Add the delete request to the ConcurrentBag to delete later
                    itemsToDelete.Add(new DeleteRequest(createResponse.EntityReference));
                });

                // Calculate the duration to complete
                var secondsToCreate = (DateTime.Now - startCreate).TotalSeconds;

                Console.WriteLine($"Created {itemsToImport.Count} records in  {secondsToCreate} seconds.");



                Console.WriteLine($"\nDeleting {itemsToDelete.Count} records");
                var startDelete = DateTime.Now;

                // Delete the records in parallel
                await Parallel.ForEachAsync(itemsToDelete, parallelOptions, async (deleteRequest, token) =>
                {
                    await service.SendAsync(deleteRequest);
                });

                // Calculate the duration to complete
                var secondsToDelete = (DateTime.Now - startDelete).TotalSeconds;

                Console.WriteLine($"Deleted {itemsToDelete.Count} records in {secondsToDelete} seconds.");

            }
            catch (Exception)
            {
                throw;
            }
            Console.WriteLine("--Parallel Operations complete--");

        }

        private static Bucket<HttpRequestMessage>[] CreateBuckets(List<CreateRequest> createRequests, int numberOfBuckets)
        {
            Bucketizer<HttpRequestMessage> bucketizer = new Bucketizer<HttpRequestMessage>();
            return bucketizer.FillBucketsEvenly(createRequests.AsEnumerable<HttpRequestMessage>(), numberOfBuckets);
        }

        private static Bucket<HttpRequestMessage>[] CreateDeleteBuckets(ConcurrentBag<DeleteRequest> deleteRequests, int numberOfBuckets)
        {
            Bucketizer<HttpRequestMessage> bucketizer = new Bucketizer<HttpRequestMessage>();
            return bucketizer.FillBucketsEvenly(deleteRequests.AsEnumerable<HttpRequestMessage>(), numberOfBuckets);
        }

        private static async Task RunParallellBatch(Service service, ParallelOptions parallelOptions, Bucket<HttpRequestMessage>[] buckets)
        {
            // ConcurrentBag is a thread-safe, unordered collection of objects.
            ConcurrentBag<DeleteRequest> itemsToDelete = new();

            try
            {
                Console.WriteLine($"Creating {buckets.Count()} requests with {numberOfRecords} records");
                var startCreate = DateTime.Now;

                // Send the requests in parallel
                await Parallel.ForEachAsync(buckets, parallelOptions, async (bucket, token) =>
                {
                    ChangeSet change = new ChangeSet(bucket.Items.ToList());
                    List<ChangeSet> changeSets = new List<ChangeSet>();
                    changeSets.Add(change);

                    BatchRequest batch = new(service.BaseAddress);
                    batch.ChangeSets = changeSets;

                    var batchResponse = await service.SendAsync<BatchResponse>(batch);

                    var httpResponses = batchResponse.HttpResponseMessages;

                    foreach (HttpResponseMessage httpResponse in httpResponses)
                    {
                        if (httpResponse != null)
                        {
                            var entityRef = httpResponse.GetEntityReference();

                            if (entityRef != null)
                            {
                                // Add the delete request to the ConcurrentBag to delete later
                                itemsToDelete.Add(new DeleteRequest(httpResponse.GetEntityReference()));
                            }
                        }
                    }
                });

                // Calculate the duration to complete
                var secondsToCreate = (DateTime.Now - startCreate).TotalSeconds;

                Console.WriteLine($"Created {buckets.Count()} requests with {numberOfRecords} records in  {secondsToCreate} seconds.");

                await Cleanup(service, parallelOptions, itemsToDelete, buckets.Count());

            }
            catch (Exception)
            {
                throw;
            }
            Console.WriteLine("--Parallel Operations complete--");

        }

        private static async Task Cleanup(Service service, ParallelOptions parallelOptions, ConcurrentBag<DeleteRequest> itemsToDelete, int numberOfBuckets)
        {
            if (numberOfRecords / numberOfBuckets > 32)
            {
                // max 32 records per delete
                numberOfBuckets = numberOfRecords / 32;
            }

            Console.WriteLine($"\nDeleting {itemsToDelete.Count} accounts in {numberOfBuckets} requests");
            var deleteBuckets = CreateDeleteBuckets(itemsToDelete, numberOfBuckets);

            var startDelete = DateTime.Now;

            // Delete the accounts in parallel
            await Parallel.ForEachAsync(deleteBuckets, parallelOptions, async (bucket, token) =>
            {
                ChangeSet change = new ChangeSet(bucket.Items.ToList());
                List<ChangeSet> changeSets = new List<ChangeSet>();
                changeSets.Add(change);

                BatchRequest batch = new(service.BaseAddress);
                batch.ChangeSets = changeSets;

                var batchResponse = await service.SendAsync<BatchResponse>(batch);
            });

            // Calculate the duration to complete
            var secondsToDelete = (DateTime.Now - startDelete).TotalSeconds;

            Console.WriteLine($"Deleted {itemsToDelete.Count} accounts in {secondsToDelete} seconds.");
        }
    }


}