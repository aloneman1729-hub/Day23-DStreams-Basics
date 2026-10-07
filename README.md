# Day 23 — DStreams Basics

## Overview

This project demonstrates Apache Spark Streaming using DStreams.

The application simulates streaming application logs through a TCP socket and processes the incoming data in micro-batches.

## Scenario

Application logs are continuously received from a socket.

The Spark Streaming application:

1. Reads text data from the socket.
2. Splits incoming lines into words using `flatMap`.
3. Converts words into key-value pairs using `map`.
4. Filters only `ERROR` messages using `filter`.
5. Counts the number of `ERROR` messages in each micro-batch.

## Technologies

* Scala 2.12.18
* Apache Spark 3.5.1
* Spark Streaming / DStreams
* SBT
* Netcat
* Java 17

## Project Structure

```text
Day23-DStreams-Basics/
├── build.sbt
├── README.md
├── .gitignore
└── src/
    └── main/
        └── scala/
            └── com/
                └── day23dstreams/
                    └── LogStreamingApp.scala
```

## DStreams Processing Flow

```text
Socket Stream
     |
     v
Text Lines
     |
     v
flatMap
     |
     v
map
     |
     v
filter ERROR
     |
     v
reduceByKey
     |
     v
ERROR Count
```

## Micro-Batch Processing

The application uses a **5-second batch interval**.

Spark Streaming divides continuously arriving data into small batches. Each batch is processed as a separate Spark job.

For example, if a batch receives:

```text
INFO Application started
ERROR Database connection failed
INFO User logged in
ERROR File not found
```

The application counts:

```text
(ERROR,2)
```

The next 5-second interval creates another micro-batch and the process repeats.

## Key DStreams Operations

### `flatMap`

Splits each incoming log line into individual words.

### `map`

Transforms each word into a key-value pair:

```text
(ERROR, 1)
```

### `filter`

Keeps only records where the word is `ERROR`.

### `reduceByKey`

Combines the values for the same key and produces the total ERROR count.

## Running the Project

### Terminal 1 — Start the socket

```bash
nc -lk 9999
```

### Terminal 2 — Start the Spark application

```bash
sbt run
```

### Send sample logs

In the socket terminal:

```text
INFO Application started
ERROR Database connection failed
INFO User logged in
ERROR File not found
INFO Request completed
```

The Spark application processes the logs in 5-second micro-batches.

Expected output:

```text
(ERROR,2)
```

## Learning Outcomes

This project demonstrates:

* Creating a `StreamingContext`
* Configuring a batch interval
* Reading a socket stream
* DStreams
* `map`
* `filter`
* `flatMap`
* `reduceByKey`
* Micro-batch processing
* Real-time ERROR log counting
* Starting and stopping a Spark Streaming application
