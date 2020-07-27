using System;
using System.Threading;
using System.Diagnostics;
using FASTER.core;

namespace FASTER.microbench
{
    class DeleteTest
    {
        static readonly int kPerRoundKey = (1 << 20);
        static readonly int kRound = (1 << 8);
        static readonly int kThread = 4;
        static readonly int kMaxKey = (kPerRoundKey * kRound);
        static IDevice device;
        static FasterKV<Key, Value, Input, Output, Empty, Functions> store;
        static long total_insert = 0;
        static long total_delete = 0;

        static private void upsertWorker(int threadId, Key[][] keys, Value[][] values)
        {
            long tick = 0;
            var session = store.NewSession();
            for (long i = threadId; i < kRound; i += kThread)
            {
                for (long j = 0; j < kPerRoundKey; j++)
                {
                    session.Upsert(ref keys[i][j], ref values[i][j], Empty.Default, 1);
                    tick++;
                }
            }
            session.Dispose();
            Interlocked.Add(ref total_insert, tick);
            //Console.Out.WriteLine(threadId + ":" + tick);
        }

        private static void deleteWorker(int threadId, Key[][] keys, Value[][] values)
        {
            long tick = 0;
            var session = store.NewSession();
            for (long i = threadId; i < kRound; i += kThread)
            {
                for (long j = 0; j < kPerRoundKey; j++)
                {
                    session.Delete(ref keys[i][j], Empty.Default, 1);
                    tick++;
                }
            }
            session.Dispose();
            Interlocked.Add(ref total_delete, tick);
        }

        public static void Main(string[] args)
        {
            device = new NullDevice();

            store = new FasterKV<Key, Value, Input, Output, Empty, Functions>(kMaxKey / 2, new Functions(), new LogSettings { LogDevice = device });

            Console.WriteLine("UpsertDelete total: " + kMaxKey + ":" + kPerRoundKey + "*" + kRound + " thread: " + kThread);
            Key[][] keys = new Key[kRound][];
            Value[][] values = new Value[kRound][];
            for (long i = 0; i < kRound; i++)
            {
                keys[i] = new Key[kPerRoundKey];
                values[i] = new Value[kPerRoundKey];
            }
            for (long i = 0; i < kMaxKey; i++)
            {
                keys[i / kPerRoundKey][i % kPerRoundKey] = new Key { value = i };
                values[i / kPerRoundKey][i % kPerRoundKey] = new Value { value = i };
                //store.Upsert(new Key { value = i }, i, Empty.Default, 1);
            }
            for (int r = 0; r < 1; r++)
            {
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();
                Thread[] threads = new Thread[kThread];
                for (int tid = 0; tid < kThread; tid++)
                {
                    int threadId = tid;
                    threads[tid] = new Thread(() => upsertWorker(threadId, keys, values));
                }
                for (int tid = 0; tid < kThread; tid++)
                {
                    threads[tid].Start();
                }
                for (int tid = 0; tid < kThread; tid++)
                {
                    threads[tid].Join();
                }
                stopwatch.Stop();
                Console.WriteLine("Round" + r + "\t->" + total_insert + "(-)" + store.EntryCount + "{-}" + store.IndexSize + "$-$" + stopwatch.ElapsedMilliseconds);

                stopwatch.Restart();
                var session = store.NewSession();
                session.Refresh();
                session.CompletePending(false);
                session.Dispose();
                stopwatch.Stop();
                Console.WriteLine("\t" + "->" + total_insert + "(-)" + store.EntryCount + "{-}" + store.IndexSize + "$-$" + stopwatch.ElapsedMilliseconds);

                stopwatch.Restart();
                for (int tid = 0; tid < kThread; tid++)
                {
                    int threadId = tid;
                    threads[tid] = new Thread(() => deleteWorker(threadId, keys, values));
                }
                for (int tid = 0; tid < kThread; tid++)
                {
                    threads[tid].Start();
                }
                for (int tid = 0; tid < kThread; tid++)
                {
                    threads[tid].Join();
                }
                stopwatch.Stop();
                Console.WriteLine("\t" + "<-" + total_delete + "(-)" + store.EntryCount + "{-}" + store.IndexSize + "$-$" + stopwatch.ElapsedMilliseconds);

                stopwatch.Restart();
                session = store.NewSession();
                session.Refresh();
                session.CompletePending(false);
                session.Dispose();
                stopwatch.Stop();
                Console.WriteLine("\t" + "<-" + total_delete + "(-)" + store.EntryCount + "{-}" + store.IndexSize + "$-$" + stopwatch.ElapsedMilliseconds);

                session = store.NewSession();
                session.Refresh();
                session.CompletePending(false);
                Console.Out.WriteLine("\tHeadAddress:" + store.Log.HeadAddress);
                Console.Out.WriteLine("\tBeginAddress:" + store.Log.BeginAddress);
                Console.Out.WriteLine("\tReadOnlyAddress:" + store.Log.ReadOnlyAddress);
                Console.Out.WriteLine("\tSafeReadOnlyAddress:" + store.Log.SafeReadOnlyAddress);
                Console.Out.WriteLine("\tTailAddress:" + store.Log.TailAddress);
                stopwatch.Restart();
                session.CompletePending(true);
                //store.Log.Compact(store.Log.BeginAddress);
                //store.TakeFullCheckpoint(out var checkpointGuid);
                //store.CompleteCheckpoint(true);
                session.Dispose();
                stopwatch.Stop();
                Console.Out.WriteLine("\t\tCheckoutPoint time:" + stopwatch.ElapsedMilliseconds + "{-}" /*+ checkpointGuid*/);
            }
        }
    }
}
