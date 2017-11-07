﻿namespace Nata.IO.Consul

open System
open System.Text
open Consul
open Nata.IO

type Client = IKVEndpoint

type WriteException(s) =
    inherit Exception(s)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Client =

    let create { Settings.Address=address
                 Settings.DataCenter=dataCenter } =
        let client = new Consul.ConsulClient(fun configuration ->
            configuration.Address <- new Uri(address)
            configuration.Datacenter <- dataCenter)
        client.KV
    
    let toEvent (prefix:string) : KVPair -> Event<KeyValue> option =
        function
        | null -> None
        | pair ->
            (pair.Key.Substring(prefix.Length), pair.Value)
            |> Event.create
            |> Some

    let ofEvent : Event<KeyValue> -> Consul.KVPair =
        Event.data >> fun (key,value) ->
            let pair = new Consul.KVPair(key)
            pair.Value <- value
            pair
        
    let read (client:Client) prefix = 
        let result =
            client.List(prefix)
            |> Async.AwaitTask
            |> Async.RunSynchronously
        result.Response
        |> Seq.choose (toEvent prefix)

    let write (client:Client) prefix (event:Event<KeyValue>) =
        let result =
            event
            |> Event.mapData (fun (k,v) ->
                if k.StartsWith(prefix) then k, v
                elif prefix.EndsWith("/") then prefix + k, v
                else prefix + "/" + k, v)
            |> ofEvent
            |> client.Put
            |> Async.AwaitTask
            |> Async.RunSynchronously
        if not result.Response then
            let message =
                sprintf "Write failed with status %A after %A"
                <| result.StatusCode
                <| result.RequestTime
            raise(new WriteException(message))
        

    let connect (settings:Settings) =
        let client = create settings
        fun prefix ->
            [
                Capability.Reader <| fun () ->
                    read client prefix

                Capability.Writer <|
                    write client prefix
            ]