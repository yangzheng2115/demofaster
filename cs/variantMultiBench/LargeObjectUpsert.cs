﻿using System;
using System.Threading;
using System.Diagnostics;
using FASTER.core;

namespace FASTER.variantMultiBench
{
    class LargeObjectUpsert
    {
        static readonly bool updateOnly = true;
        static readonly bool needVerify = false;
        static readonly int kPerRoundKey = (1 << 20);
        static readonly int kRound = (1 << 8);
        static readonly int objectWideth = (1 << 4);
        static readonly int kThread = 4;
        static readonly int kMaxKey = (kPerRoundKey * kRound);
        static IDevice device;
        static FasterKV<Key, LargeValue, Input, LargeOutput, Empty, LargeFunctions> store;
        static long total_count = 0;

        static private void upsertWorker(int threadId, Key[][] keys, LargeValue[][] values)
        {
            var sesstion = store.NewSession();
            long tick = 0;
            for (long i = threadId; i < kRound; i += kThread)
            {
                for (long j = 0; j < kPerRoundKey; j++)
                {
                    sesstion.Upsert(ref keys[i][j], ref values[i][j], Empty.Default, 1);
                    tick++;
                }
            }
            sesstion.Dispose();
            Interlocked.Add(ref total_count, tick);
        }

        static private void readWorker(int threadId, Key[][] keys)
        {
            var sesstion = store.NewSession();
            long tick = 0;
            Input input = default(Input);
            LargeOutput output = new LargeOutput();
            for (long i = threadId; i < kRound; i += kThread)
            {
                for (long j = 0; j < kPerRoundKey; j++)
                {
                    sesstion.Read(ref keys[i][j], ref input, ref output, Empty.Default, 1);
                    if (needVerify)
                    {
                        long v0 = BitConverter.ToInt64(output.value.value, 0);
                        for (int k = 0; k < objectWideth; i += sizeof(long))
                        {
                            long v = BitConverter.ToInt64(output.value.value, k);
                            if (v0 != v)
                            {
                                Console.WriteLine(threadId + ":" + v + "<->" + v0);
                                goto closeSession;
                            }
                        }
                    }
                    tick++;
                }
            }
        closeSession:
            sesstion.Dispose();
            Interlocked.Add(ref total_count, tick);
        }

        public static void Main(string[] args)
        {
            device = new NullDevice();

            store = new FasterKV<Key, LargeValue, Input, LargeOutput, Empty, LargeFunctions>(kMaxKey / 2,
              new LargeFunctions(), new LogSettings { LogDevice = device, MemorySizeBits = 32 },
              new CheckpointSettings { CheckpointDir = null, CheckPointType = CheckpointType.Snapshot },
              new SerializerSettings<Key, LargeValue> { keySerializer = () => new KeySerializer(), valueSerializer = () => new LargeValueSerializer() });

            Console.WriteLine("BigObject total: " + kMaxKey + ":" + kPerRoundKey + "*" + kRound + " thread: " + kThread);
            for (long r = 0; r < 1; r++)
            {
                Stopwatch stopwatch = new Stopwatch(); stopwatch.Start();
                Key[][] keys = new Key[kRound][];
                LargeValue[][] values = new LargeValue[kRound][];
                for (long i = 0; i < kRound; i++)
                {
                    keys[i] = new Key[kPerRoundKey];
                    values[i] = new LargeValue[kPerRoundKey];
                }
                for (long i = 0; i < kMaxKey; i++)
                {
                    if (updateOnly)
                    {
                        keys[i / kPerRoundKey][i % kPerRoundKey] = new Key { key = (int)i };
                    }
                    else
                    {
                        keys[i / kPerRoundKey][i % kPerRoundKey] = new Key { key = (int)(r * kMaxKey + i) };
                    }
                    values[i / kPerRoundKey][i % kPerRoundKey] = new LargeValue(r * kMaxKey + i, objectWideth);
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
                Console.WriteLine("Round update" + r + "<->" + total_count + "(-)" + store.EntryCount + "{-}" + store.IndexSize + "$-$" + stopwatch.ElapsedMilliseconds);

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

                var sesstion = store.NewSession();
                sesstion.Refresh();
                sesstion.CompletePending(true);
                //Console.Out.WriteLine("\t\t<--" + store.Log.TailAddress);
                Console.Out.WriteLine("\tHeadAddress:" + store.Log.HeadAddress);
                Console.Out.WriteLine("\tBeginAddress:" + store.Log.BeginAddress);
                Console.Out.WriteLine("\tReadOnlyAddress:" + store.Log.ReadOnlyAddress);
                Console.Out.WriteLine("\tSafeReadOnlyAddress:" + store.Log.SafeReadOnlyAddress);
                Console.Out.WriteLine("\tTailAddress:" + store.Log.TailAddress);
                sesstion.CompletePending();
                //store.Log.Compact(store.Log.BeginAddress);
                //store.CompleteCheckpoint();
                sesstion.Dispose();
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
