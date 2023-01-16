using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKA.Dataverse.ConsoleApp
{
    internal class Bucket<T>
    {
        internal IList<T> Items { get; } = new List<T>();
        internal int TotalCost { get; set; } = 0;

        private Func<T, int> cost;

        internal Bucket(Func<T, int> cost)
        {
            this.cost = cost;
        }

        internal void Add(T item)
        {
            Items.Add(item);
            TotalCost += cost(item);
        }
    }
}
