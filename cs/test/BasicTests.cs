// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using NUnit.Framework;

namespace FASTER.VariantVerification
{
    [TestFixture]
    internal class BasicTests
    {
        private static byte[] longToByte(long Num)
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
        private static byte[] intToByte(int Num)
        {
            byte[] abyte = new byte[4];
            int j = 0xff;
            int z = 8;
            for (int i = 0; i < 4; i++)
            {
                long y = j << z * i;
                long x = Num & y;
                x = x >> (z * i);
                abyte[i] = (byte)(x);
            }

            return abyte;
        }

        [Test]
        public void TranslateTest()
        {
            long lv = 13212;
            byte[] bv = longToByte(lv);
            long tv = BitConverter.ToInt64(bv, 0);
            Assert.AreEqual(lv, tv);
            byte[] lbv = new byte[64];
            for (int i = 0; i < 64; i += 8)
            {
                Array.Copy(bv, 0, lbv, i, 8);
            }
            for (int i = 0; i < 64; i += 8)
            {
                long l = BitConverter.ToInt64(lbv, i);
                Assert.AreEqual(l, lv);
            }
            int iv = 1048576;
            byte[] ibv = intToByte(iv);
            int itv = System.BitConverter.ToInt32(ibv, 0);
            Assert.AreEqual(iv, itv);
        }

        [Test]
        public void TypeWidthTest()
        {
            int iv = 10;
            Console.WriteLine(sizeof(int) + ":" + sizeof(long) + ":" + sizeof(short) + ":" + sizeof(byte));
            Assert.AreEqual(iv, 10);
            Assert.AreEqual(sizeof(int), 4);
            Assert.AreEqual(sizeof(long), 8);
            Assert.AreEqual(sizeof(short), 2);
            Assert.AreEqual(sizeof(byte), 1);
        }
    }
}
