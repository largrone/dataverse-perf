using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKA.Dataverse.ConsoleApp
{
    internal class Bucketizer<T>
    {

        internal Bucket<T>[] FillBucketsEvenly(IEnumerable<T> items, int bucketCount)
        {
            int itemsPerBucket = items.Count() / bucketCount;
            int countOfBucketsTakingExtraOne = items.Count() % bucketCount;

            Func<T, int> cost = x => 1;

            Bucket<T>[] buckets = new Bucket<T>[bucketCount];
            int itemsAdded = 0;
            for (int bucketPos = 0; bucketPos < bucketCount; bucketPos++)
            {
                buckets[bucketPos] = new Bucket<T>(cost);
                int grabSize = itemsPerBucket + (bucketPos < countOfBucketsTakingExtraOne ? 1 : 0);

                var grab = items.Skip(itemsAdded).Take(grabSize);
                for (int grabPos = 0; grabPos < grabSize; grabPos++)
                {
                    buckets[bucketPos].Add(grab.ElementAt(grabPos));
                    itemsAdded += cost(grab.ElementAt(grabPos));
                }

            }
            return buckets;

        }
    }
}
