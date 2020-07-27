// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.VariantVerification
{
    public class Key : IFasterEqualityComparer<Key>
    {
        public int key;

        public long GetHashCode64(ref Key key)
        {
            return Utility.GetHashCode(key.key);
        }

        public bool Equals(ref Key k1, ref Key k2)
        {
            return k1.key == k2.key;
        }
    }

    public class KeySerializer : BinaryObjectSerializer<Key>
    {
        public override void Deserialize(ref Key obj)
        {
            obj.key = reader.ReadInt32();
        }

        public override void Serialize(ref Key obj)
        {
            writer.Write(obj.key);
        }
    }

    public class LargeValue
    {
        public static byte[] longToByte(long Num)
        {
            byte[] abyte = new byte[8];
            int j = 0xff;
            int z = 8;
            for (int i = 0; i < 8; i++)
            {
                long y = j << z * i;
                long x = Num & y;
                x = x >> (z * i);
                abyte[i] = (byte)(x);
            }

            return abyte;
        }

        public byte[] value;

        public LargeValue()
        {

        }

        public LargeValue(int size)
        {
            value = new byte[size];
            for (int i = 0; i < size; i++)
            {
                value[i] = (byte)(size + i);
            }
        }

        public LargeValue(long content, int size)
        {
            value = new byte[size];
            for (int i = 0; i < size; i += sizeof(long))
            {
                Array.Copy(LargeValue.longToByte(content), 0, value, i, sizeof(long));
            }
        }
    }

    public class LargeValueSerializer : BinaryObjectSerializer<LargeValue>
    {
        public override void Deserialize(ref LargeValue obj)
        {
            int size = reader.ReadInt32();
            obj.value = reader.ReadBytes(size);
        }

        public override void Serialize(ref LargeValue obj)
        {
            writer.Write(obj.value.Length);
            writer.Write(obj.value);
        }
    }
    public class Input
    {
        public int value;
    }

    public class LargeOutput
    {
        public LargeValue value;
    }

    public class LargeFunctions : IFunctions<Key, LargeValue, Input, LargeOutput, Empty>
    {
        public void RMWCompletionCallback(ref Key key, ref Input input, Empty ctx, Status status)
        {
        }

        public void ReadCompletionCallback(ref Key key, ref Input input, ref LargeOutput output, Empty ctx, Status status)
        {
            Assert.IsTrue(status == Status.OK);
            for (int i = 0; i < output.value.value.Length; i++)
            {
                Assert.IsTrue(output.value.value[i] == (byte)(output.value.value.Length + i));
            }
        }

        public void UpsertCompletionCallback(ref Key key, ref LargeValue value, Empty ctx)
        {
        }

        public void DeleteCompletionCallback(ref Key key, Empty ctx)
        {
        }

        public void CopyUpdater(ref Key key, ref Input input, ref LargeValue oldValue, ref LargeValue newValue)
        {
        }

        public void InitialUpdater(ref Key key, ref Input input, ref LargeValue value)
        {
        }

        public bool InPlaceUpdater(ref Key key, ref Input input, ref LargeValue value)
        {
            return true;
        }

        public void SingleReader(ref Key key, ref Input input, ref LargeValue value, ref LargeOutput dst)
        {
            dst.value = value;
        }

        public void ConcurrentReader(ref Key key, ref Input input, ref LargeValue value, ref LargeOutput dst)
        {
            dst.value = value;
        }

        public bool ConcurrentWriter(ref Key key, ref LargeValue src, ref LargeValue dst)
        {
            dst = src;
            return true;
        }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        {
         }

        public void SingleWriter(ref Key key, ref LargeValue src, ref LargeValue dst)
        {
            dst = src;
        }
    }

    [TestFixture]
    internal class FASTERKVTest
    {
        private FasterKV<Key, LargeValue, Input, LargeOutput, Empty, LargeFunctions> fht;

        [SetUp]
        public void Setup()
        {
            fht = new FasterKV<Key, LargeValue, Input, LargeOutput, Empty, LargeFunctions>
                ((1 << 20), new LargeFunctions(), new LogSettings { LogDevice = new NullDevice(), MemorySizeBits = 32 },
                      new CheckpointSettings { CheckpointDir = null, CheckPointType = CheckpointType.Snapshot },
                      new SerializerSettings<Key, LargeValue> { keySerializer = () => new KeySerializer(), valueSerializer = () => new LargeValueSerializer() });
        }

        [TearDown]
        public void TearDown()
        {
            fht.Dispose();
            fht = null;
        }

        [Test]
        public void SingleUpsert()
        {
            UpsertWorker();
            var session = fht.NewSession();
            int iv = 10;
            Console.WriteLine(sizeof(int) + ":" + sizeof(long) + ":" + sizeof(short) + ":" + sizeof(byte));
            Assert.AreEqual(iv, 10);
            Assert.AreEqual(sizeof(int), 4);
            Assert.AreEqual(sizeof(long), 8);
            Assert.AreEqual(sizeof(short), 2);
            Assert.AreEqual(sizeof(byte), 1);
            Key key = new Key { key = -1231 };
            LargeValue value = new LargeValue(8);
            session.Upsert(ref key, ref value, Empty.Default, 1);
            LargeValue value1 = new LargeValue(13212, 64);
            session.Upsert(ref key, ref value1, Empty.Default, 1);
            Input input = default(Input);
            LargeOutput output = new LargeOutput();
            session.Read(ref key, ref input, ref output, Empty.Default, 1);
            for (int i = 0; i < 64; i += sizeof(long))
            {
                long v = BitConverter.ToInt64(output.value.value, i);
                Assert.AreEqual(v, 13212);
            }
            DeleteWorker(ref session);
            session.Dispose();
        }

        int tick = 0;

        bool begin = false;

        void UpsertWorker()
        {
            while (!begin) Thread.Yield();
            var session = fht.NewSession();
            Key key = new Key { key = 0 };
            for (int i = 0; i < UpdateCount; i++)
            {
                //Console.WriteLine(i + ":" + key);
                LargeValue value = new LargeValue((long)i, 64);
                session.Upsert(ref key, ref value, Empty.Default, 1);
                Interlocked.Increment(ref tick);
            }
            session.Dispose();
        }

        void InitWorker()
        {
            var session = fht.NewSession();
            Key key = new Key { key = 0 };
            LargeValue value = new LargeValue((long)0, 64);
            session.Upsert(ref key, ref value, Empty.Default, 1);
            session.Dispose();
        }

        void DeleteWorker(ref ClientSession<Key, LargeValue, Input, LargeOutput, Empty, LargeFunctions> session)
        {
            Key key = new Key { key = 0 };
            session.Delete(ref key, Empty.Default, 1);
        }

        void ReadWorker()
        {
            var session = fht.NewSession();
            Key key = new Key { key = 0 };
            int old = 0;
            int read = 0;
            begin = true;
            while (tick < UpdateNumber * UpdateCount)
            {
                Input input = default(Input);
                LargeOutput output = new LargeOutput();
                session.Read(ref key, ref input, ref output, Empty.Default, 1);
                long df = BitConverter.ToInt64(output.value.value, 0);
                if (tick - old < UpdateNumber * UpdateCount / ReadCount) continue;
                old = tick;
                if (read % 8 == 0)
                {
                    TestContext.WriteLine();
                }
                TestContext.Write(read + ":" + df + "\t");
                for (int j = 0; j < 64; j += sizeof(long))
                {
                    long v = BitConverter.ToInt64(output.value.value, j);
                    Assert.AreEqual(v, df);
                }
                read++;
            }
            session.Dispose();
        }

        private readonly int UpdateNumber = 8;
        private readonly int ReadNumber = 1;
        private readonly int UpdateCount = (1 << 22);
        private readonly int ReadCount = (1 << 8);

        [Test]
        public void ConcurrentUpsert()
        {
            InitWorker();
            Thread[] updater = new Thread[UpdateNumber];
            Thread[] reader = new Thread[ReadNumber];
            TestContext.WriteLine(reader.Length);
            for (int tid = 0; tid < UpdateNumber; tid++)
            {
                int threadId = tid;
                updater[tid] = new Thread(() => UpsertWorker());
                updater[tid].Start();
            }
            for (int tid = 0; tid < ReadNumber; tid++)
            {
                reader[tid] = new Thread(() => ReadWorker());
                reader[tid].Start();
            }
            for (int tid = 0; tid < UpdateNumber; tid++)
            {
                updater[tid].Join();
            }
            for (int tid = 0; tid < ReadNumber; tid++)
            {
                reader[tid].Join();
            }
        }
    }
}