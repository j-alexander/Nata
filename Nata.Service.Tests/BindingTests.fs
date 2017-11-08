﻿namespace Nata.Service.Tests

open System
open System.Text
open FSharp.Data
open NUnit.Framework
open Nata.Core
open Nata.IO
open Nata.IO.Capability
open Nata.IO.Memory
open Nata.Service

[<TestFixture>]
type BindingTests() =

    let channel() =
        Stream.connect()
        <| guid()

    let snapshot channel =
        let readFrom = Channel.readerFrom channel
        readFrom(Position.Before Position.End)
        |> Seq.map (fst >> Event.data >> Consumer.state)
        |> Seq.head

    [<Test>]
    member x.TestFold() =
        let input, output =
            channel(), channel()
        let fn state x =
            let xs = Option.defaultValue [] state
            x :: xs
        let writeAll =
            let write = Event.create >> Channel.writer input
            List.rev >> List.iter write
        let list = ["a"; "b"; "c"]
        list |> writeAll
        let states =
            input
            |> Binding.fold fn output
            |> Seq.take 3
            |> Seq.toList
        let result = snapshot output
        Assert.AreEqual(list, result)
        Assert.AreEqual(
            [["c"];["b";"c"];["a";"b";"c"]],
            states
            |> Seq.map Consumer.state
            |> Seq.toList)

    [<Test>]
    member x.TestMap() =
        let input, output =
            channel(), channel()
        let fn x = x * 2
        [1..5]
        |> List.iter (Event.create >> Channel.writer input)
        let states =
            input
            |> Binding.map fn output
            |> Seq.take 5
            |> Seq.map Consumer.state
            |> Seq.toList
        Assert.AreEqual([2;4;6;8;10], states)
        Assert.AreEqual(10, snapshot output)

    [<Test>]
    member x.TestBifold() =
        let output, left, right =
            channel(), channel(), channel()
        let fn state x =
            let state = Option.defaultValue [] state
            x :: state
        let consume =
            let bifold = Binding.bifold fn output (left,right)
            fun () ->
                bifold
                |> Seq.head
                |> Consumer.state
        let writeLeft, writeRight =
            Event.create >> Channel.writer left,
            Event.create >> Channel.writer right
        let check (expect:Choice<int,string> list) =
            let result = consume()
            Assert.AreEqual(expect, result)
        writeLeft 1
        check[Choice1Of2 1]
        writeRight "2"
        check[Choice2Of2 "2"
              Choice1Of2 1]
        writeLeft 3
        check[Choice1Of2 3
              Choice2Of2 "2"
              Choice1Of2 1]
        writeLeft 4
        check[Choice1Of2 4
              Choice1Of2 3
              Choice2Of2 "2"
              Choice1Of2 1]
        writeRight "5"
        check[Choice2Of2 "5"
              Choice1Of2 4
              Choice1Of2 3
              Choice2Of2 "2"
              Choice1Of2 1]

    [<Test>]
    member x.TestBimap() =
        let output, left, right =
            channel(), channel(), channel()
        let fn =
            function
            | Choice1Of2 l -> 2 * l
            | Choice2Of2 r -> 3 * r
        let consume =
            let bimap = Binding.bimap fn output (left,right)
            fun () ->
                bimap
                |> Seq.head
                |> Consumer.state
        let writeLeft, writeRight =
            Event.create >> Channel.writer left,
            Event.create >> Channel.writer right
        let check (expect:int) =
            let result = consume()
            Assert.AreEqual(expect, result)
        writeLeft 1
        check 2
        writeRight 2
        check 6
        writeLeft 3
        check 6
        writeRight 4
        check 12
        writeLeft 5
        check 10
        writeRight 6
        check 18
        writeLeft 7
        check 14

    [<Test>]
    member x.TestMultifold() =
        let fn state x =
            let state = Option.defaultValue 0 state
            x + state
        let output = channel()
        let inputs =
            [ "A", channel()
              "B", channel()
              "C", channel() ]
        let consume =
            let multifold = Binding.multifold fn output inputs
            fun () ->
                multifold
                |> Seq.head
                |> Consumer.state
        let writeA, writeB, writeC =
            Event.create >> (snd inputs.[0] |> Channel.writer),
            Event.create >> (snd inputs.[1] |> Channel.writer),
            Event.create >> (snd inputs.[2] |> Channel.writer)
        let check (expect:int) =
            let result = consume()
            Assert.AreEqual(expect, result)
        writeA 1
        check(1)
        writeB 2
        check(1+2)
        writeC 3
        check(1+2+3)
        writeB 4
        check(1+2+3+4)
        writeA 5
        check(1+2+3+4+5)

    [<Test>]
    member x.TestMultimap() =
        let fn x = (x%2) + 3
        let output = channel()
        let inputs =
            [ "A", channel()
              "B", channel()
              "C", channel() ]
        let consume =
            let multimap = Binding.multimap fn output inputs
            fun () ->
                multimap
                |> Seq.head
                |> Consumer.state
        let writeA, writeB, writeC =
            Event.create >> (snd inputs.[0] |> Channel.writer),
            Event.create >> (snd inputs.[1] |> Channel.writer),
            Event.create >> (snd inputs.[2] |> Channel.writer)
        let check (expect:int) =
            let result = consume()
            Assert.AreEqual(expect, result)
        writeA 7
        check 4
        writeB 8
        check 3
        writeC 5
        check 4
        writeB 9
        check 4