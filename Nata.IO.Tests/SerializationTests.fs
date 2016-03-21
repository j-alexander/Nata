﻿namespace Nata.IO.Tests

open System
open System.Reflection
open System.Text
open FSharp.Data
open NUnit.Framework

open Nata.IO
open Nata.IO.Capability
open Nata.IO.JsonValue.Codec

type DataType = {
    case : string
    at : DateTime
} with
    static member ToBytes : Codec<DataType,byte[]> = createTypeToBytes()
    static member OfBytes : Codec<byte[],DataType> = createBytesToType()

type MetadataType = {
    from : string
} with
    static member ToBytes : Codec<MetadataType,byte[]> = createTypeToBytes()
    static member OfBytes : Codec<byte[],MetadataType> = createBytesToType()

[<AbstractClass>]
type SerializationTests() =

    abstract member Connect : unit -> Source<string,byte[],byte[],int64>
    abstract member Channel : unit -> string

    [<Test>]
    member x.TestSerializationCodecs() =
        let connection =
            x.Connect()
            |> Source.map DataType.ToBytes MetadataType.ToBytes
        
        let write, read =
            let stream = connection <| x.Channel()
            writer stream,
            reader stream

        let events =
            [ for i in 0..10 ->
                { Data =
                    { DataType.case = sprintf "Event-%d" i
                      DataType.at = DateTime.Now }
                  Metadata =
                    { MetadataType.from = Assembly.GetExecutingAssembly().FullName }
                  Date = DateTime.UtcNow
                  Stream = null
                  Type = "AutomaticSerialization" } ]

        for event in events do
            write event

        for (event, result) in read() |> Seq.zip events do
            Assert.AreEqual(event.Data.case, result.Data.case)
            Assert.AreEqual(event.Metadata.from, result.Metadata.from)