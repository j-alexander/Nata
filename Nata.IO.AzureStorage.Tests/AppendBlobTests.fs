namespace Nata.IO.AzureStorage.Tests

open System
open System.IO
open System.Collections.Concurrent
open System.Reflection
open System.Threading
open System.Threading.Tasks
open FSharp.Data
open NUnit.Framework

open Nata.Core
open Nata.Core.JsonValue.Codec
open Nata.IO
open Nata.IO.Capability
open Nata.IO.AzureStorage
open Nata.IO.AzureStorage.Blob.Append

[<TestFixture(Description="Azure-AppendBlob")>]
type AppendBlobTests() =

    let container() =
        Emulator.Account.connectionString
        |> Account.create 
        |> Blob.Container.create (guid())

    let event() =
        [| "text", JsonValue.String (guid()) |]
        |> JsonValue.Record
        |> Event.create

    [<Test>]
    member x.TestReadWrite() =
        let container, event = container(), event()
        let blob_name = guid()

        do write container blob_name event
        let result =
            read container blob_name
            |> Seq.head

        Assert.AreEqual(event.Data, result.Data)
        Assert.True(result |> Event.name |> Option.isSome)
        