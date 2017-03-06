﻿namespace Nata.IO.Tests

open System
open System.Collections.Concurrent
open System.Reflection
open System.Text
open FSharp.Data
open NUnit.Framework

open Nata.Core
open Nata.IO
open Nata.IO.Capability

[<AbstractClass>]
type LogStoreTests() as x =

    let event(fn) =
        [| 
            "case", JsonValue.String (x.GetType().Name + "." + fn)
            "at", JsonValue.String (DateTime.Now.ToString()) 
            "from", JsonValue.String (Assembly.GetExecutingAssembly().FullName)
        |]
        |> JsonValue.Record
        |> JsonValue.toBytes       
        |> Event.create
        |> Event.withEventType fn

    abstract member Connect : unit -> Source<string,byte[],int64>
    abstract member Channel : unit -> string

    member private x.Capabilities() = x.Channel() |> x.Connect()

    [<Test>]
    member x.TestConnect() =
        let stream = x.Capabilities()
        Assert.Greater(stream.Length, 0)

    [<Test>]
    member x.TestWrite() =
        let write = x.Capabilities() |> writer
        let event = event("TestWrite")
        write event

    [<Test; Timeout(45000)>]
    member x.TestEmptyRead() =
        let read = x.Capabilities() |> reader
        Assert.AreEqual([], read() |> Seq.toList)

    [<Test>]
    member x.TestRead() =
        let connection = x.Capabilities()
        let write = writer connection
        let read = reader connection
        let event = event("TestRead")
        write event

        let result = read() |> Seq.head
        Assert.AreEqual(event.Data, result.Data)

    [<Test; Timeout(45000)>]
    member x.TestEmptyReadFrom() =
        let readFrom = x.Capabilities() |> readerFrom
        Assert.AreEqual([], readFrom Position.Start |> Seq.toList)

    [<Test>]
    member x.TestReadFrom() =
        let connection = x.Capabilities()
        let write = writer connection
        let readFrom = readerFrom connection
        let event_0 = event("TestReadFrom-0")
        let event_1 = event("TestReadFrom-1")
        let event_2 = event("TestReadFrom-2")
        write event_0
        write event_1
        write event_2
        
        let result, index_0 = readFrom (Position.Start) |> Seq.head
        Assert.GreaterOrEqual(index_0, 0L)
        Assert.AreEqual(event_0.Data, result.Data)
        
        let result, index_1 = readFrom (Position.At (1L+index_0)) |> Seq.head
        Assert.Greater(index_1, index_0)
        Assert.AreEqual(event_1.Data, result.Data)
        
        let result, index_2 = readFrom (Position.At (1L+index_1)) |> Seq.head
        Assert.Greater(index_2, index_1)
        Assert.AreEqual(event_2.Data, result.Data)

        let result, index = readFrom (Position.Start) |> Seq.head
        Assert.AreEqual(event_0.Data, result.Data)

    [<Test>]
    member x.TestWriteTo() =
        let connection = x.Capabilities()
        match tryWriterTo connection with
        | Some writeTo ->
            [ 0L, event "TestWriteTo-0" |> writeTo (Position.At 0L)
              1L, event "TestWriteTo-1" |> writeTo (Position.At 1L)
              2L, event "TestWriteTo-2" |> writeTo (Position.At 2L) ]
            |> List.iter Assert.AreEqual
        | _ ->
            Assert.Ignore("WriterTo is reported to be unsupported by this source.")

    [<Test>]
    member x.TestWriteToPosition() =
        let connection = x.Capabilities()
        match tryWriterTo connection, tryReaderFrom connection with
        | Some writeTo, Some readFrom ->
            let event_0, event_1, event_2 =
                event "TestWriteTo-0",
                event "TestWriteTo-1",
                event "TestWriteTo-2"
            [ 0L, event_0 |> writeTo Position.Start
              1L, event_1 |> writeTo Position.End
              2L, event_2 |> writeTo Position.End ]
            |> List.iter Assert.AreEqual
            match readFrom Position.Start |> Seq.toList with
            | [ result_0, 0L ; result_1, 1L ; result_2, 2L ] ->
                Assert.AreEqual(event_0.Data, result_0.Data)
                Assert.AreEqual(event_1.Data, result_1.Data)
                Assert.AreEqual(event_2.Data, result_2.Data)
            | _ -> Assert.Fail()
        | _ ->
            Assert.Ignore("ReaderFrom and WriterTo are reported to be unsupported by this source.")

    [<Test>]
    member x.TestWriteToShouldFailWithIndexTooLow() =
        let connection = x.Capabilities()
        match tryWriterTo connection with
        | Some writeTo ->
            event "TestWriteToShouldFailWithIndexTooLow-0" |> writeTo (Position.At -1L) |> ignore
            Assert.Throws<Position.Invalid<int64>>(fun _ ->
                event "TestWriteToShouldFailWithIndexTooLow-1" |> writeTo (Position.At 0L) |> ignore
            ) |> ignore
            Assert.Throws<Position.Invalid<int64>>(fun _ ->
                event "TestWriteToShouldFailWithIndexTooLow-2" |> writeTo (Position.At 0L) |> ignore
            ) |> ignore
        | _ ->
            Assert.Ignore("WriterTo is reported to be unsupported by this source.")

    [<Test>]
    member x.TestWriteToShouldFailWithIndexTooHigh() =
        let connection = x.Capabilities()
        match tryWriterTo connection with
        | Some writeTo ->
            event "TestWriteToShouldFailWithIndexTooHigh-0" |> writeTo (Position.At -1L) |> ignore
            Assert.Throws<Position.Invalid<int64>>(fun _ ->
                event "TestWriteToShouldFailWithIndexTooHigh-1" |> writeTo (Position.At 0L) |> ignore
            ) |> ignore
            Assert.Throws<Position.Invalid<int64>>(fun _ ->
                event "TestWriteToShouldFailWithIndexTooHigh-2" |> writeTo (Position.At 2L) |> ignore
            ) |> ignore
        | _ ->
            Assert.Ignore("WriterTo is reported to be unsupported by this source.")
        
    [<Test; Timeout(120000)>]
    member x.TestLiveSubscription() =
        let write, subscribe =
            let connection = x.Capabilities()
            writer connection, subscriber connection
        let results = subscribe()
        let expected =
            [ event "TestLiveSubscription-0"
              event "TestLiveSubscription-1"
              event "TestLiveSubscription-2" ]
        for event in expected do
            write event
        results
        |> Seq.take 3
        |> Seq.toList
        |> List.zip expected
        |> List.iter(fun (expected, actual) ->
            Assert.AreEqual(expected.Data, actual.Data))
            
    [<Test; Timeout(120000)>]
    member x.TestLateSubscription() =
        let write, subscribe =
            let connection= x.Capabilities()
            writer connection, subscriber connection
        let expected =
            [ event "TestLateSubscription-0"
              event "TestLateSubscription-1"
              event "TestLateSubscription-2" ]
        for event in expected do
            write event
        subscribe()
        |> Seq.take 3
        |> Seq.toList
        |> List.zip expected
        |> List.iter(fun (expected, actual) ->
            Assert.AreEqual(expected.Data, actual.Data))
            
    [<Test; Timeout(120000)>]
    member x.TestSubscriptionFromIndex() =
        let write, subscribeFrom =
            let connection = x.Capabilities()
            writer connection, subscriberFrom connection
        let expected =
            [| event "TestSubscriptionFromIndex-0"
               event "TestSubscriptionFromIndex-1"
               event "TestSubscriptionFromIndex-2" |]
        for event in expected do
            write event
        let indexes =
            subscribeFrom (Position.Start)
            |> Seq.take 3
            |> Seq.map snd
            |> Seq.toList
        subscribeFrom (Position.Start)
        |> Seq.take 3
        |> Seq.toArray
        |> Array.zip expected
        |> Array.iter(fun (expected, (actual, index)) ->
            Assert.AreEqual(expected.Data, actual.Data))
        subscribeFrom (Position.At (indexes.[0]))
        |> Seq.take 3
        |> Seq.toArray
        |> Array.zip expected
        |> Array.iter(fun (expected, (actual, index)) ->
            Assert.AreEqual(expected.Data, actual.Data))
        subscribeFrom (Position.At (indexes.[1]))
        |> Seq.take 2
        |> Seq.toArray
        |> Array.zip (expected.[1..2])
        |> Array.iter(fun (expected, (actual, index)) ->
            Assert.AreEqual(expected.Data, actual.Data))
        subscribeFrom (Position.At (indexes.[2]))
        |> Seq.take 1
        |> Seq.toArray
        |> Array.zip (expected.[2..2])
        |> Array.iter(fun (expected, (actual, index)) ->
            Assert.AreEqual(expected.Data, actual.Data))
            
    [<Test; Timeout(120000)>]
    member x.TestSubscriptionContinuation() =
        let write, subscribeFrom =
            let connection = x.Capabilities()
            writer connection,
            subscriberFrom connection

        let flag,stop =
            event "TestSubscriptionContinuation-flag",
            event "TestSubscriptionContinuation-stop"
        let results = new BlockingCollection<byte[]>()
        let subscriber =
            async {
                subscribeFrom Position.End
                |> Seq.takeWhile (fst >> Event.data >> (=) (Event.data flag))
                |> Seq.iter (fst >> Event.data >> results.Add)
            } |> Async.StartAsTask
              |> Async.AwaitTask

        let rec publish() =
            async {
                do! Async.Sleep(1000)
                match results |> Seq.tryFind ((=) (Event.data flag)) with
                | None ->
                    write flag
                    return! publish()
                | Some x ->
                    write stop
                    return ()
            }
            
        publish()
        |> Async.RunSynchronously
        subscriber
        |> Async.RunSynchronously

    [<Test>]
    member x.TestIndexEmpty() =
        match x.Capabilities() |> tryIndexer with
        | None ->
            Assert.Ignore("Indexer is reported to be unsupported by this source.")
        | Some index ->
            Assert.AreEqual(0, index Position.Start)
            Assert.AreEqual(0, index Position.End)

    [<Test>]
    member x.TestIndexWithWrites() =
        let tryIndex, tryWrite =
            let stream = x.Capabilities()
            tryIndexer stream,
            tryWriter stream
        match tryIndex, tryWrite with
        | Some index, Some write ->
            Assert.AreEqual(0, index Position.Start)
            Assert.AreEqual(0, index Position.End)
            write(event("TestIndexWithWrites-0"))
            Assert.AreEqual(0, index Position.Start)
            Assert.AreEqual(1, index Position.End)
            write(event("TestIndexWithWrites-1"))
            Assert.AreEqual(0, index Position.Start)
            Assert.AreEqual(2, index Position.End)
            write(event("TestIndexWithWrites-2"))
            Assert.AreEqual(0, index Position.Start)
            Assert.AreEqual(3, index Position.End)
        | _ ->
            Assert.Ignore("Indexer or Writer is reported to be unsupported by this source.")
