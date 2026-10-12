# MessageServer Architecture Documentation

## Overview

MessageServer is a lightweight pub/sub and queue messaging server built with Micronaut 4.2.0 that leverages Server-Sent Events (SSE) for real-time message delivery. The system provides both ephemeral (topics) and durable (queues) messaging patterns, making it suitable for building event-driven applications.

## Technology Stack

- **Framework**: Micronaut 4.2.0
- **Language**: Java 20
- **Reactive**: RxJava 2 for reactive streams
- **Transport**: Server-Sent Events (SSE) for push notifications
- **Persistence**: Oracle NoSQL KV Store
- **Build Tool**: Maven
- **Logging**: Logback

## Project Structure

```
MessageServer/
├── src/main/java/messageserver/
│   ├── MessageServer.java              # Application entry point
│   ├── TopicController.java            # REST endpoints for topics
│   ├── QueueController.java            # REST endpoints for queues
│   ├── sse/
│   │   ├── Messenger.java              # Base messaging manager
│   │   ├── Destination.java            # Abstract destination base
│   │   ├── Topic.java                  # Pub/sub topic implementation
│   │   ├── Queue.java                  # Queue implementation
│   │   └── Message.java                # Message data structure
│   └── persist/
│       ├── MessagePersistance.java     # Persistence interface
│       ├── MessagePersistanceFactory.java
│       └── MessageNoSQL.java           # Oracle NoSQL implementation
├── src/main/resources/
│   └── logback.xml                     # Logging configuration
├── pom.xml                             # Maven dependencies
└── application.yml                     # Micronaut configuration
```

## Core Components

### 1. Application Entry Point

**File**: `MessageServer.java`

The main class that bootstraps the Micronaut application. Contains utility methods for logging and system monitoring using JMX MBeans.

```java
public static void main(String[] args) {
    Micronaut.run(MessageServer.class);
}
```

### 2. REST API Controllers

#### TopicController

**Endpoint Base**: `/messageserver/api`

**Responsibilities**:
- Manages pub/sub topic subscriptions and publications
- Extends `Messenger` for destination management

**Endpoints**:

| Method | Path | Description |
|--------|------|-------------|
| GET | `/topic/{name}` | Subscribe to a topic and receive messages via SSE |
| POST | `/topic/publish?name={name}` | Publish a message to a topic |

**Key Implementation Details**:
- Uses `Flowable.generate()` to create reactive SSE streams
- Broadcasts messages to all active subscribers
- Non-persistent - messages are lost if no subscribers are active

#### QueueController

**Endpoint Base**: `/messageserver/api/`

**Responsibilities**:
- Manages queue-based message delivery
- Persists messages for durability
- Loads saved messages on startup

**Endpoints**:

| Method | Path | Description |
|--------|------|-------------|
| GET | `/queue/{name}` | Listen to a queue and receive messages via SSE |
| POST | `/queue/publish?name={name}` | Send a message to a queue |

**Key Implementation Details**:
- Messages are persisted to Oracle NoSQL before delivery
- Single consumer model - each message delivered to at most one listener
- Messages deleted after successful delivery
- Automatically restores queued messages on server restart

### 3. Messaging Model

#### Messenger (Base Class)

**Location**: `messageserver.sse.Messenger`

**Responsibilities**:
- Maintains concurrent map of all destinations (topics and queues)
- Factory methods for obtaining/creating destinations
- Message processing logic

**Key Methods**:
```java
Queue getQueue(String name)
Topic getTopic(String name)
HttpResponse processSend(Destination dest, String data)
```

#### Destination (Abstract Base)

**Location**: `messageserver.sse.Destination`

**Responsibilities**:
- Common interface for topics and queues
- Maintains destination name
- Manages listener vector

**Abstract Methods**:
```java
abstract Message getNextMessage() throws InterruptedException
abstract boolean addMessage(String data)
```

#### Topic Implementation

**Location**: `messageserver.sse.Topic`

**Pattern**: Pub/Sub (Broadcast)

**Characteristics**:
- Non-persistent (messages not saved)
- All subscribers receive every message
- Uses Java wait/notify for message distribution
- Last message stored temporarily

**Flow**:
1. Subscribers call `getNextMessage()` and block on `wait()`
2. Publisher calls `addMessage()` which triggers `notifyAll()`
3. All waiting subscribers wake up and receive the message

#### Queue Implementation

**Location**: `messageserver.sse.Queue`

**Pattern**: Point-to-Point

**Characteristics**:
- Persistent (messages saved to NoSQL)
- Single consumer - one listener receives each message
- Uses `ArrayBlockingQueue` (capacity: 20,000 message IDs)
- Messages deleted after delivery

**Flow**:
1. Publisher sends message → persisted to NoSQL → ID added to queue
2. Listener calls `getNextMessage()` → blocks on `take()` until message available
3. Message retrieved from NoSQL using ID
4. Message delivered and deleted from storage

### 4. Persistence Layer

#### MessagePersistance (Interface)

**Location**: `messageserver.persist.MessagePersistance`

**Methods**:
```java
boolean saveMessage(String destinationName, Long messageId, String message)
Message getMessage(String destinationName, Long messageId)
boolean deleteMessage(Long messageId, String destinationName)
ArrayList<String> getStoredDestinations()
ArrayList<Long> getMessageIds(String destinationName)
```

#### MessageNoSQL (Implementation)

**Location**: `messageserver.persist.MessageNoSQL`

**Storage**: Oracle NoSQL KV Store

**Configuration**:
- Store Name: `kvstore`
- Host: `127.0.0.1`
- Port: `5000`

**Key Structure**:
- Major Key: Destination name (queue name)
- Minor Key: Message ID (timestamp in milliseconds)

**Singleton Pattern**: Ensures single database connection instance

## Message Flow Diagrams

### Topic (Pub/Sub) Flow

```
Publisher                  Topic                   Subscriber 1, 2, 3
    |                        |                            |
    |-- POST /publish ------>|                            |
    |                        |-- wait() -----------------> |
    |                        |                            |
    |-- message data ------->|                            |
    |                        |-- notifyAll() ------------>|
    |<-- HTTP 200 OK --------|                            |
    |                        |-- SSE event ------------->|
    |                        |                            |
```

### Queue Flow

```
Publisher              Queue              NoSQL Store       Consumer
    |                    |                      |              |
    |-- POST /publish -->|                      |              |
    |                    |-- saveMessage ------>|              |
    |                    |<-- success ----------|              |
    |                    |-- add ID to queue -->|              |
    |<-- HTTP 200 OK ----|                      |              |
    |                    |                      |              |
    |                    |<-- take() (blocking)--------------- |
    |                    |-- getMessage ------->|              |
    |                    |<-- message data -----|              |
    |                    |-- SSE event ----------------------->|
    |                    |-- deleteMessage ---->|              |
    |                    |<-- deleted ----------|              |
```

## API Reference

### Subscribe to Topic

```http
GET /messageserver/api/topic/{topicName}
Accept: text/event-stream
```

**Response**: SSE stream with messages

```
event: message
data: {message content}
```

### Publish to Topic

```http
POST /messageserver/api/topic/publish?name={topicName}
Content-Type: text/event-stream

event: message
id: {optional-id}
data: {message content}
```

### Listen to Queue

```http
GET /messageserver/api/queue/{queueName}
Accept: text/event-stream
```

**Response**: SSE stream with messages (one per consumer)

### Send to Queue

```http
POST /messageserver/api/queue/publish?name={queueName}
Content-Type: text/event-stream

event: message
id: {optional-id}
data: {message content}
```

## Key Design Patterns

### 1. Factory Pattern
- `MessagePersistanceFactory` - Creates persistence layer instances
- `Messenger.getQueue()` / `getTopic()` - Lazy creation of destinations

### 2. Singleton Pattern
- `MessagePersistanceFactory` - Single factory instance
- `MessageNoSQL` - Single database connection

### 3. Strategy Pattern
- `MessagePersistance` interface with `MessageNoSQL` implementation
- Allows swapping storage backends without changing queue logic

### 4. Reactive Streams
- RxJava `Flowable` for SSE event generation
- Backpressure-aware message delivery

## Configuration Files

### pom.xml

Key dependencies:
- `micronaut-http-server-netty` - HTTP server
- `micronaut-session` - Session management
- `rxjava` 2.2.21 - Reactive streams
- `kvclient_3.0.5` - Oracle NoSQL client
- `jackson-databind` - JSON processing

Build plugins:
- `maven-shade-plugin` - Creates uber JAR
- `exec-maven-plugin` - Run application directly

### application.yml

```yaml
micronaut:
  logger:
    levels:
      ERROR
```

Minimal configuration - most settings use Micronaut defaults

### logback.xml

Console logging with pattern:
```
%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n
```

Root level: `info`

## Concurrency Considerations

### Thread Safety

1. **Destinations Map**: Uses `ConcurrentHashMap` for thread-safe access
2. **Queue Message IDs**: `ArrayBlockingQueue` provides thread-safe blocking operations
3. **Topic Synchronization**: `synchronized` blocks on monitor object for wait/notify
4. **Listeners Vector**: Legacy `Vector` (thread-safe but older design)

### Blocking Operations

- `Queue.getNextMessage()` - Blocks until message available (uses `ArrayBlockingQueue.take()`)
- `Topic.getNextMessage()` - Blocks until new message published (uses `Object.wait()`)

## Potential Improvements

### Performance
- Replace `Vector` with `CopyOnWriteArrayList` for better concurrent read performance
- Consider connection pooling for NoSQL store
- Add caching layer for frequently accessed messages

### Reliability
- Implement dead letter queue for failed deliveries
- Add message TTL (time-to-live) support
- Transaction support for message operations

### Features
- Message filtering/routing based on content
- Priority queues
- Delayed message delivery
- Message batching

### Operations
- Health checks and metrics endpoints
- Admin API for queue/topic management
- Monitoring and alerting integration

## Running the Application

### Prerequisites
- Java 20 or higher
- Maven 3.x
- Oracle NoSQL KV Store running on localhost:5000

### Build
```bash
mvn clean package
```

### Run
```bash
java -jar target/MessageServer-1.0-SNAPSHOT.jar
```

Or use the Maven exec plugin:
```bash
mvn exec:exec
```

### Run Script
```bash
./run
```

## Dependencies

### Core Dependencies
- Micronaut 4.2.0
- Java 20
- RxJava 2.2.21
- Oracle KV Client 3.0.5

### Testing
- JUnit Jupiter (JUnit 5)
- Micronaut Test framework

## Author

Eric J. Bruno (ericjbruno)

## License

Not specified in project files
