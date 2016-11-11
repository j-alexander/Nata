﻿namespace Nata.IO.EventHub

open System
open Microsoft.ServiceBus.Messaging
open Nata.Core
open Nata.IO

type Group = EventHubConsumerGroup
type Receiver = EventHubReceiver

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Receiver =

    let toSeqWithOffset (wait:TimeSpan option)
                        (group:Group)
                        (startAt:Index option)
                        (partition:PartitionString) =
        seq {

            let receiver =
                match startAt with
                | None -> group.CreateReceiver(partition)
                | Some start -> group.CreateReceiver(partition, Index.toString (start-1L))
                
            let receive _ =
                match wait with
                | Some max -> receiver.Receive(max)
                | None -> receiver.Receive()

            use connection =
                { new IDisposable with
                    member x.Dispose() = if not receiver.IsClosed then receiver.Close() }
                    
            let partition = Partition.parse partition

            yield!
                Seq.unfold(receive >> function null -> None | x -> Some(x,())) ()
                |> Seq.map(fun data ->
                    let index = 
                        data.Offset
                        |> Index.parse
                    data.GetBytes()
                    |> Event.create
                    |> Event.withPartition partition
                    |> Event.withIndex index
                    |> Event.withSentAt data.EnqueuedTimeUtc,
                    { Offset.Partition = partition
                      Offset.Index = index })
        }

    let toSeqWithIndex wait group startAt partition =
        toSeqWithOffset wait group startAt partition
        |> Seq.mapSnd Offset.index

    let toSeq wait group startAt partition =
        toSeqWithOffset wait group startAt partition
        |> Seq.map fst
