using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Threading.Tasks;
using Microsoft.Reactive.Testing;
using Xunit;

namespace PolyStreams
{
    class Program
    {
        public static string[] SourceIds = { "a", "b", "c", "d" };

        static void Main(string[] args)
        {
            var queries = GenerateStream(Scheduler.Default);

            queries.Subscribe(Console.WriteLine);

            Console.ReadLine();
        }

        public static IObservable<string> GenerateStream(IScheduler scheduler)
        {
            var padding = TimeSpan.FromSeconds(0.5);
            var repeatAfter = TimeSpan.FromTicks(padding.Ticks * SourceIds.Length)
                              + TimeSpan.FromSeconds(5);

            // I want to repeatedly query a set of things ("sources")
            // for their current state. However I don't want to query 
            // them all at the exact same time, so I stagger them out
            // a bit.
            // First, I'm generating a finite stream of the source
            // spread out over some time.
            var sources = Observable.Generate(
                0,
                i => i < SourceIds.Length,
                i => i + 1,
                i => SourceIds[i],
                _ => padding,
                scheduler);

            // For each source in the originating stream, I want to keep
            // querying it at a regular interval.
            var queries = sources.SelectMany(id =>
            {
                var repeatingQueries = Observable
                    .Interval(repeatAfter, scheduler)
                    .Select(_ => id);

                // I don't want to wait _before_ issuing my first query
                // so I prepend a single event without any delay.
                return Observable
                    .Return(id)
                    .Concat(repeatingQueries)
                    .SelectMany(partitionId => QueryAsync(partitionId).ToObservable());
                // Since my query is an aysnc operation, I use 
                // `ToObservable` and `SelectMany`.
                // It's okay if the responses in the resulting stream are
                // not in the order that the queries were issued.
            });
            return queries;
        }

        private static Task<string> QueryAsync(string sourceId)
        {
            return Task.FromResult(sourceId + ": result");
        }
    }

    public class DoesMyStreamWork
    {
        [Fact]
        public void WithVirtualTime()
        {
            var expected = Program.SourceIds
                .Select(x => x + ": result")
                .ToList();

            var actual = new List<string>();
            var time = new TestScheduler();
            var stream = Program.GenerateStream(time);
            stream.Take(4).Subscribe(x => actual.Add(x));

            // Since I'm expecting 4 value with 0.5 in between, 
            // I wait just a _little bit_ longer...
            time.AdvanceBy(TimeSpan.FromSeconds(2.1).Ticks);

            Assert.Equal(expected, actual);
        }
    }
}
