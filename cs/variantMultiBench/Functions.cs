// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

using System;
using FASTER.core;

namespace FASTER.variantMultiBench
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
            for (int i = 0; i < output.value.value.Length; i++)
            {
                if (output.value.value[i] == (byte)(output.value.value.Length + i))
                {
                    continue;
                }
                else
                {
                    //Console.WriteLine(i + ":" + output.value.value);
                }
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
}
