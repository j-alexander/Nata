namespace Nata.IO.AzureStorage

open System
open System.IO
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Blob
open Nata.Core
open Nata.IO

module Blob =

    module Container =

        type Name = string

        let create (containerName:Name) (account:Account) =
            let client = account.CreateCloudBlobClient()
            let container = client.GetContainerReference(containerName)
            container.CreateIfNotExistsAsync()
            |> Task.wait
            container

    module Block =

        type Name = string
    
        let write (container:CloudBlobContainer) (blobName:Name) =
            let reference = container.GetBlockBlobReference(blobName)
            fun (event:Event<byte[]>) ->
                reference.UploadFromByteArrayAsync(event.Data, 0, event.Data.Length)
                |> Task.wait

        let writeTo (container:CloudBlobContainer) (blobName:Name) =
            let reference = container.GetBlockBlobReference(blobName)
            fun (position:Position<string>) ->
                let condition = function
                    | Position.Start -> AccessCondition.GenerateIfNotExistsCondition()
                    | Position.Before x -> AccessCondition.GenerateEmptyCondition()
                    | Position.At etag -> AccessCondition.GenerateIfMatchCondition(etag)
                    | Position.After x -> AccessCondition.GenerateEmptyCondition()
                    | Position.End -> AccessCondition.GenerateEmptyCondition()
                fun (event:Event<byte[]>) ->
                    let etag = reference.Properties.ETag
                    while etag = reference.Properties.ETag do
                        reference.UploadFromByteArrayAsync(event.Data, 0, event.Data.Length, condition position, null, null)
                        |> Task.wait
                    reference.Properties.ETag

        let readFrom (container:CloudBlobContainer) (blobName:Name) =
            let reference = container.GetBlockBlobReference(blobName)
            let condition = function
                | Position.Start -> AccessCondition.GenerateIfNotExistsCondition()
                | Position.Before x -> AccessCondition.GenerateEmptyCondition()
                | Position.At etag -> AccessCondition.GenerateIfMatchCondition(etag)
                | Position.After x -> AccessCondition.GenerateEmptyCondition()
                | Position.End -> AccessCondition.GenerateEmptyCondition()
            Seq.unfold <| fun (position:Position<string>) ->
                try 
                    use memory = new MemoryStream()
                    reference.DownloadToStreamAsync(memory, condition position, null, null)
                    |> Task.wait
                    let bytes = memory.ToArray()
                    let created =
                        reference.Properties.LastModified
                        |> Nullable.map DateTime.ofOffset
                    let etag, contentType =
                        reference.Properties.ETag,
                        reference.Properties.ContentType
                    let event =
                        Event.create bytes
                        |> Event.withName blobName
                        |> Event.withTag etag
                        |> Event.withEventType contentType
                        |> Event.withCreatedAtNullable created
                    Some ((event, etag), position)
                with
                | :? StorageException as e when
                    [ 400   // 400 - bad request (also, blob does not yet exist)
                      404   // 404 - blob does not yet exist
                      412 ] // 412 - etag for blob has expired or is invalid
                    |> List.exists ((=) e.RequestInformation.HttpStatusCode) -> None

        let read container blobName =
            readFrom container blobName Position.End |> Seq.map fst

        let connect : Connector<Account*Container.Name,Name,byte[],string> =

            fun (account,containerName) ->

                let container =
                    Container.create containerName account

                fun (blob:Name) ->
                    [ 
                        Capability.Reader <| fun () ->
                            read container blob

                        Capability.ReaderFrom <|
                            readFrom container blob

                        Capability.Writer <|
                            write container blob

                        Capability.WriterTo <|
                            writeTo container blob
                    ]
            
    module Append =

        type Name = string

        open FSharp.Data

        let encode, decode = Event.Codec.EventToString
        let tryDecode line =
            try Some (decode line)
            with _ -> None
    
        let write (container:CloudBlobContainer) (blobName:Name) =
            let reference = container.GetAppendBlobReference(blobName)
            reference.CreateOrReplaceAsync(AccessCondition.GenerateIfNotExistsCondition(), null, null)
            |> Task.wait
            fun x ->
                reference.AppendTextAsync(encode(x))
                |> Task.wait

        let read (container:CloudBlobContainer) (blobName:Name) =
            let reference = container.GetAppendBlobReference(blobName)
            seq {
                use stream =
                    reference.OpenReadAsync()
                    |> Task.waitForResult
                use reader = new StreamReader(stream)
                while reader.EndOfStream |> not do
                    yield
                        reader.ReadLine()
                        |> decode
                        |> Event.withName blobName
            }

        let connect : Connector<Account*Container.Name,Name,JsonValue,string> =

            fun (account,containerName) ->

                let container =
                    Container.create containerName account

                fun (blob:Name) ->
                    [ 
                        Capability.Reader <| fun () ->
                            read container blob

                        Capability.Writer <|
                            write container blob
                    ]