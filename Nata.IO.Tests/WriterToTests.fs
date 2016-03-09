﻿namespace Nata.IO.Tests

open System
open System.Text
open FSharp.Data
open NUnit.Framework
open Nata.IO
open Nata.IO.Capability

[<AbstractClass>]
type WriterToTests() =

    let event =
        { Type = "event_type"
          Stream = "event_stream"
          Date = DateTime.Now
          Data = ()
          Metadata = () }

    abstract member Connect : unit -> List<Capability<'Data,'Metadata,int>>
    
    member x.Connect(fn) =
        let stream = x.Connect()
        stream |> reader,
        stream |> writerTo |> fn

    [<Test>]
    member x.MapDataValueTest() =
        let read, writeTo = x.Connect(WriterTo.mapData ((+) 1))

        let input = [1;2;3]
        let output = [2;3;4]

        let run =
            for at, i in input |> Seq.mapi (fun at i -> at-1,i) do
                event |> Event.mapData (fun _ -> i) |> writeTo at |> ignore
            read >> Seq.map Event.data >> Seq.toList

        Assert.AreEqual(output, run())

    [<Test>]
    member x.MapDataTypeTest() =
        let read, writeTo = x.Connect(WriterTo.mapData (fun x -> x.ToString()))

        let input = [1;2;3]
        let output = ["1";"2";"3"]

        let run =
            for at, i in input |> Seq.mapi (fun at i -> at-1,i) do
                event |> Event.mapData (fun _ -> i) |> writeTo at |> ignore
            read >> Seq.map Event.data >> Seq.toList

        Assert.AreEqual(output, run())

    [<Test>]
    member x.MapMetadataValueTest() =
        let read, writeTo = x.Connect(WriterTo.mapMetadata (fun i -> i*i))

        let input = [1;2;3]
        let output = [1;4;9]

        let run =
            for at, i in input |> Seq.mapi (fun at i -> at-1,i) do
                event |> Event.mapMetadata (fun _ -> i) |> writeTo at |> ignore
            read >> Seq.map Event.metadata >> Seq.toList

        Assert.AreEqual(output, run())

    [<Test>]
    member x.MapMetadataTypeTest() =
        let read, writeTo = x.Connect(WriterTo.mapMetadata int64)

        let input = [1;2;3]
        let output = [1L;2L;3L]

        let run =
            for at, i in input |> Seq.mapi (fun at i -> at-1,i) do
                event |> Event.mapMetadata (fun _ -> i) |> writeTo at |> ignore
            read >> Seq.map Event.metadata >> Seq.toList

        Assert.AreEqual(output, run())

    [<Test>]
    member x.MapTest() =
        let mapping = WriterTo.map (fun (x:int) -> int64 (x*x)) (fun (x:int) -> (1+x).ToString()) Codec.Identity
        let read, writeTo = x.Connect(mapping)

        let input = [1;2;3;4]

        let outputData = [1L;4L;9L;16L]
        let outputMetadata = ["2";"3";"4";"5"]

        let results =
            for at, i in input |> Seq.mapi (fun at i -> at-1,i) do
                event |> Event.map (fun _ -> i) (fun _ -> i) |> writeTo at |> ignore
            read() |> Seq.toList


        Assert.AreEqual(outputData, results |> List.map Event.data)
        Assert.AreEqual(outputMetadata, results |> List.map Event.metadata)

