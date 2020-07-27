using System;
using System.Threading;
using System.Diagnostics;
using FASTER.core;

namespace FASTER.multibench
{
    class IterativeUpsert
    {
        static readonly bool updateOnly = true;
        static readonly int kPerRoundKey = (1 << 20);
        static readonly int kRound = (1 << 8);
        static readonly int kThread = 4;
        static readonly int kMaxKey = (kPerRoundKey * kRound);
        static IDevice device;
        static FasterKV<Key, Value, Input, Output, Empty, Functions> store;
        static long total_count = 0;

        static private void upsertWorker(int threadId, Key[][] keys, Value[][] values)
        {
            var session = store.NewSession();
            long tick = 0;
            for (long i = threadId; i < kRound; i += kThread)
            {
                for (long j = 0; j < kPerRoundKey; j++)
                {
                    session.Upsert(ref keys[i][j], ref values[i][j], Empty.Default, 1);
                    tick++;
                }
            }
            session.Dispose();
            Interlocked.Add(ref total_count, tick);
        }

        static private void readWorker(int threadId, Key[][] keys)
        {
            var session = store.NewSession();
            long tick = 0;
            Input input = default(Input);
            Output output = new Output();
            for (long i = threadId; i < kRound; i += kThread)
            {
                for (long j = 0; j < kPerRoundKey; j++)
                {
                    session.Read(ref keys[i][j], ref input, ref output, Empty.Default, 1);
                    tick++;
                }
            }
            session.Dispose();
            Interlocked.Add(ref total_count, tick);
        }

        public static void Main(string[] args)
        {
            device = new NullDevice();

            store = new FasterKV<Key, Value, Input, Output, Empty, Functions>(kMaxKey / 2, new Functions(), new LogSettings { LogDevice = device });

            Console.WriteLine("IterativeUpsert total: " + kMaxKey + ":" + kPerRoundKey + "*" + kRound + " thread: " + kThread);
            for (long r = 0; r < 1; r++)
            {
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();
                Key[][] keys = new Key[kRound][];
                Value[][] values = new Value[kRound][];
                for (long i = 0; i < kRound; i++)
                {
                    keys[i] = new Key[kPerRoundKey];
                    values[i] = new Value[kPerRoundKey];
                }
                for (long i = 0; i < kMaxKey; i++)
                {
                    if (updateOnly)
                    {
                        keys[i / kPerRoundKey][i % kPerRoundKey] = new Key { value = i };
                    }
                    else
                    {
                        keys[i / kPerRoundKey][i % kPerRoundKey] = new Key { value = r * kMaxKey + i };
                    }
                    values[i / kPerRoundKey][i % kPerRoundKey] = new Value { value = r * kMaxKey + i };
                }
                stopwatch.Stop();
                Console.WriteLine("Round generate" + r + "<->" + total_count + "(-)" + store.EntryCount + "{-}" + store.IndexSize + "$-$" + stopwatch.ElapsedMilliseconds);
                stopwatch.Restart();
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
                Console.WriteLine("Round" + r + "<->" + total_count + "(-)" + store.EntryCount + "{-}" + store.IndexSize + "$-$" + stopwatch.ElapsedMilliseconds);

                total_count = 0;
                stopwatch.Restart();
                threads = new Thread[kThread];
                for (int tid = 0; tid < kThread; tid++)
                {
                    int threadId = tid;
                    threads[tid] = new Thread(() => readWorker(threadId, keys));
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
                Console.WriteLine("Round read" + r + "<->" + total_count + "(-)" + store.EntryCount + "{-}" + store.IndexSize + "$-$" + stopwatch.ElapsedMilliseconds);

                var session = store.NewSession();
                session.Refresh();
                session.CompletePending(true);
                //Console.Out.WriteLine("\t\t<--" + store.Log.TailAddress);
                Console.Out.WriteLine("\tHeadAddress:" + store.Log.HeadAddress);
                Console.Out.WriteLine("\tBeginAddress:" + store.Log.BeginAddress);
                Console.Out.WriteLine("\tReadOnlyAddress:" + store.Log.ReadOnlyAddress);
                Console.Out.WriteLine("\tSafeReadOnlyAddress:" + store.Log.SafeReadOnlyAddress);
                Console.Out.WriteLine("\tTailAddress:" + store.Log.TailAddress);
                session.CompletePending();
                //store.Log.Compact(store.Log.BeginAddress);
                //store.CompleteCheckpoint();
                session.Dispose();
                for (long i = 0; i < kRound; i++)
                {
                    keys = null;
                    values = null;
                }
                keys = null;
                values = null;
            }
        }
    }
}
