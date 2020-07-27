---
layout: default
title: FASTER C#
nav_order: 2
description: Overview of C# version of FASTER
permalink: /cs
---

Introduction to FASTER C#
=========================

FASTER C# works in .NET Framework and .NET core, and can be used in both a single-threaded and concurrent setting. It has been tested to work on both Windows and Linux. It exposes an API that allows one to performs a mix of Reads, Blind Updates (Upserts), and Read-Modify-Write operations. It supports data larger than memory, and accepts an `IDevice` implementation for storing logs on storage. We have provided `IDevice` implementations for local file system, but one may create new devices, for example, to write to remote file systems. Alternatively, one may mount remote storage into the local file system. FASTER  may be used as a high-performance replacement for traditional concurrent data structures such as the .NET ConcurrentDictionary, and additionally supports larger-than-memory data. It also supports checkpointing of the data structure - both incremental and non-incremental.

Table of Contents
-----------
* [Getting FASTER](#getting-faster)
* [Basic Concepts](#basic-concepts)
* [Quick End-to-End Sample](#quick-end-to-end-sample)
* [More Examples](#more-examples)
* [Checkpointing and Recovery](#checkpointing-and-recovery)

## Getting FASTER

### Building From Sources
Clone the Git repo, open cs/FASTER.sln in Visual Studio 2019, and build.

### NuGet
You can install FASTER binaries using NuGet, from Nuget.org. Right-click on your project, manage NuGet packages, browse for `Microsoft.FASTER`. Here is a [direct link](https://www.nuget.org/packages/Microsoft.FASTER).

## Basic Concepts

### FASTER Operations

FASTER supports three basic operations:
1. Read: Read data from the key-value store
2. Upsert: Blind upsert of values into the store (does not check for prior values)
3. Read-Modify-Write: Update values in store, used to implement operations such as Sum and Count.

### Constructor

Before instantiating FASTER, you need to create storage devices that FASTER will use. If you are using blittable types, you only need the hybrid log device. If you are also using objects, you need to create a separate object log device.

```cs
IDevice log = Devices.CreateLogDevice("C:\\Temp\\hybridlog_native.log");
```

Then, an instance of FASTER is created as follows:

```cs
fht = new FasterKV<Key, Value, Input, Output, Empty, Functions>
  (1L << 20, new Functions(), new LogSettings { LogDevice = log });
```

### Type Arguments to Constructor

There are six basic concepts, provided as generic type arguments when instantiating FASTER:
1. `Key`: This is the type of the key, e.g., `long`.
2. `Value`: This is the type of the value stored in FASTER.
3. `Input`: This is the type of input provided to FASTER when calling Read or RMW. It may be regarded as a parameter for the Read or RMW operation. For example, with RMW, it may be the delta being accumulated into the value.
4. `Output`: This is the type of the output of a Read operation. The reader copies the relevant parts of the Value to Output.
5. `Context`: User-defined context for the operation. Use `Empty` if there is no context necesssary.
6. `Functions`: These is a type that implemented `IFunctions<>` and provides all callbacks necessary to use FASTER.

### Callback Functions

The user provides an instance of a type that implements `IFunctions<>`. This type encapsulates all the callbacks, which are described next:

1. SingleReader and ConcurrentReader: These are used to read from the store values and copy them to Output. Single reader can assume there are no concurrent operations.
2. SingleWriter and ConcurrentWriter: These are used to write values to the store, from a source value.
3. Completion callbacks: Called when various operations complete.
4. RMW Updaters: There are three updaters that the user specifies, InitialUpdater, InPlaceUpdater, and CopyUpdater. Together, they are used to implement the RMW operation.

### Constructor Parameters

1. Hash Table Size: This the number of buckets allocated to FASTER, where each bucket is 64 bytes (size of a cache line).
2. Log Settings: These are setings related to the size of the log and devices used by the log.
3. Checkpoint Settings: These are settings related to checkpoints, such as checkpoint type and folder. Covered in the section on checkpointing [below](#checkpointing-and-recovery).
4. Serialization Settings: Used to provide custom serializers for key and value types. Serializers implement `IObjectSerializer<Key>` for keys and `IObjectSerializer<Value>` for values. *These are only needed for non-blittable types such as C# class objects.*
5. Key Equality comparer: Used for providing a better comparer for keys, implements `IFasterEqualityComparer<Key>`.

The total in-memory footprint of FASTER is controlled by the following parameters:
1. Hash table size: This parameter (the first contructor argument) times 64 is the size of the in-memory hash table in bytes.
2. Log size: The logSettings.MemorySizeBits denotes the size of the in-memory part of the hybrid log, in bits. In other words, the size of the log is 2^B bytes, for a parameter setting of B. Note that if the log points to class objects, this size does not include the size of objects, as this information is not accessible to FASTER. The older part of the log is spilled to storage.

### Sessions (Threads)

Once FASTER is instantiated, threads may use FASTER by registering themselves via the concept of a Session, using the call 

```cs
fht.StartSession();
```

At the end, the thread calls:

```cs
fht.StopSession();
```

When all threads are done operating on FASTER, you finally dispose the FASTER instance:

```cs
fht.Dispose();
```


## Quick End-To-End Sample

Following is a simple end-to-end sample where all data is in memory, so we do not worry about pending 
I/O operations. There is no checkpointing in this example as well.

```cs
public static void Test()
{
  var log = Devices.CreateLogDevice("C:\\Temp\\hlog.log");
  var fht = new FasterKV<long, long, long, long, Empty, Funcs>
    (1L << 20, new Funcs(), new LogSettings { LogDevice = log });
  fht.StartSession();
  long key = 1, value = 1, input = 10, output = 0;
  fht.Upsert(ref key, ref value, Empty.Default, 0);
  fht.Read(ref key, ref input, ref output, Empty.Default, 0);
  Debug.Assert(output == value);
  fht.RMW(ref key, ref input, Empty.Default, 0);
  fht.RMW(ref key, ref input, Empty.Default, 0);
  fht.Read(ref key, ref input, ref output, Empty.Default, 0);
  Debug.Assert(output == value + 20);
  fht.StopSession();
  fht.Dispose();
  log.Close();
}
```

Functions for this example:

```cs
public class Funcs : IFunctions<long, long, long, long, Empty>
{
  public void SingleReader(ref long key, ref long input, ref long value, ref long dst) => dst = value;
  public void SingleWriter(ref long key, ref long src, ref long dst) => dst = src;
  public void ConcurrentReader(ref long key, ref long input, ref long value, ref long dst) => dst = value;
  public void ConcurrentWriter(ref long key, ref long src, ref long dst) => dst = src;
  public void InitialUpdater(ref long key, ref long input, ref long value) => value = input;
  public void CopyUpdater(ref long key, ref long input, ref long oldv, ref long newv) => newv = oldv + input;
  public void InPlaceUpdater(ref long key, ref long input, ref long value) => value += input;
  public void UpsertCompletionCallback(ref long key, ref long value, Empty ctx) { }
  public void ReadCompletionCallback(ref long key, ref long input, ref long output, Empty ctx, Status s) { }
  public void RMWCompletionCallback(ref long key, ref long input, Empty ctx, Status s) { }
  public void CheckpointCompletionCallback(Guid sessionId, long serialNum) { }
}
```

## More Examples

Several example projects are located in [cs/playground](https://github.com/Microsoft/FASTER/tree/master/cs/playground) (available through the solution). You can also check out more samples in the unit tests in [/cs/test](https://github.com/Microsoft/FASTER/tree/master/cs/test), which can be run through the solution or using NUnit-Console. A basic YCSB benchmark is located in [cs/benchmark](https://github.com/Microsoft/FASTER/tree/master/cs/benchmark), also available through the main solution.

## Checkpointing and Recovery

FASTER supports **checkpoint-based recovery**. Every new checkpoint persists (or makes durable) additional user-operations (Read, Upsert or RMW). FASTER allows client threads to keep track of operations that have persisted and those that have not using a session-based API. 

Recall that each FASTER threads starts a session, associated with a unique Guid.
All FASTER thread operations (Read, Upsert, RMW) carry a monotonic sequence number.
At any point in time, one may call `Checkpoint` to initiate an asynchronous checkpoint of FASTER.
After calling `Checkpoint`, each FASTER thread is (eventually) notified of a sequence number, such that all operations until, and no operations after, that sequence number, are guaranteed to be persisted as part of that checkpoint. 
This sequence number can be used by the FASTER thread to clear any in-memory buffer of operations waiting to be performed.

During recovery, threads can continue their session with the same Guid using `ContinueSession`. The function returns the thread-local sequence number until which that session hash been recovered. The new thread may use this information to replay all uncommitted operations since that point.

Below, we show a simple recovery example for a single thread. 
```cs
public class PersistenceExample 
{
  private FasterKV<long, long, long, long, Empty, Funcs> fht;
  private IDevice log;
  
  public PersistenceExample() 
  {
    log = Devices.CreateLogDevice("C:\\Temp\\hlog.log");
    fht = new FasterKV<long, long, long, long, Empty, Funcs>
    (1L << 20, new Funcs(), new LogSettings { LogDevice = log });
  }
  
  public void Run()
  {
    IssuePeriodicCheckpoints();
    RunSession();
  }
  
  public void Continue()
  {
    fht.Recover();
    IssuePeriodicCheckpoints();
    ContinueSession();
  }
  
  /* Helper Functions */
  private void RunSession() 
  {
    Guid guid = fht.StartSession();
    System.IO.File.WriteAllText(@"C:\\Temp\\session1.txt", guid.ToString());
    
    long seq = 0; // sequence identifier
    
    long key = 1, input = 10;
    while(true) 
    {
      key = (seq % 1L << 20);
      fht.RMW(ref key, ref input, Empty.Default, seq);
      seq++;
    }
    // fht.StopSession() - outside infinite loop
  }
  
  private void ContinueSession() 
  {
    string guidText = System.IO.File.ReadAllText(@"C:\\Temp\session1.txt");
    Guid sessionGuid = Guid.Parse(guidText);
    
    long seq = fht.ContinueSession(sessionGuid); // recovered seq identifier
    seq++;
    
    long key = 1, input = 10;
    while(true) 
    {
      key = (seq % 1L << 20);
      fht.RMW(ref key, ref input, Empty.Default, seq);
      seq++;
    }
  }
  
  private void IssuePeriodicCheckpoints()
  {
    var t = new Thread(() => 
    {
      while(true) 
      {
        Thread.Sleep(10000);
		fht.StartSession();
        fht.TakeCheckpoint(out Guid token);
        fht.CompleteCheckpoint(token, true);
		fht.StopSession();
      }
    });
    t.Start();
  }
}
```

FASTER supports two notions of checkpointing: Snapshot and Fold-Over. The former is a full snapshot of in-memory into a separate snapshot file, whereas the latter is an _incremental_ checkpoint of the changes since the last checkpoint. Fold-Over effectively moves the read-only marker of the hybrid log to the tail, and thus all the data is persisted as part of the same hybrid log (there is no separate snapshot file). All subsequent updates are written to new hybrid log tail locations, which gives Fold-Over its incremental nature. You can find a few basic checkpointing examples [here](https://github.com/Microsoft/FASTER/blob/master/cs/test/SimpleRecoveryTest.cs) and [here](https://github.com/Microsoft/FASTER/tree/master/cs/playground/SumStore). We plan to add more examples and details going forward.
