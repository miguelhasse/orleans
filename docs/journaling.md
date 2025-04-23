# Log-structured grain storage (#9450)

Experimental framework for log-structured grain state persistence in Orleans, based on the [replicated state machine](https://en.wikipedia.org/wiki/State_machine_replication) approach. This approach focuses on appending state changes to a log rather than overwriting full state, offering potential benefits for state types with high write frequency or complex structures. A key advantage is the ability to perform atomic updates across all durable state managed by a single grain activation. The PR includes the core journaling infrastructure, several powerful built-in durable collection types and state wrappers (including IDurableValue<T>), and an initial Azure Append Blob storage provider. A volatile in-memory provider is also included for development and testing.

## Background

Traditional grain persistence often involves reading and writing the entire grain state for one or more IPersistentState<T> instances. This can become inefficient for large or frequently updated state objects, and crucially, updates to multiple IPersistentState<T> instances within a single grain are not atomic with respect to each other.

The log-structured approach provides an alternative model:

- Grain state is modeled as a composition of one or more Durable State Machines (DSMs).
- State modifications made by the grain are captured as a sequence of log entries (commands or events) and buffered in memory.
- Calling WriteStateAsync triggers the persistence of all pending log entries across all managed DSMs as a single, atomic unit to durable storage.
- The grain's in-memory state is reconstructed upon activation by replaying the ordered sequence of these log entries from the log.
- Periodically, a snapshot of the complete state across all DSMs is saved, allowing the log to be truncated for faster future recoveries.

Beyond atomic updates and potential performance benefits for certain workloads, this pattern also offers significant extensibility:

- Library developers can define custom IDurableStateMachine implementations for specialized state requirements.
- The ordered, append-only log structure provides a foundation for building higher-level features such as transactions, indexing, and custom state types for Orleans features like reminders or stream checkpointing, which can then leverage this durable, atomic log.

## Implementation Details

1. Orleans.Journaling Project: This is the central library defining the pattern and providing the core state management logic and built-in types.
   - Core Interfaces: Defines IDurableStateMachine, IStateMachineManager (manages multiple state machines within a grain activation), and IStateMachineStorage (the interface for interacting with durable storage).
   - StateMachineManager: Implements the core logic for log recovery (replaying entries/snapshots), applying log entries and snapshots, buffering pending writes in memory (LogExtentBuilder), and coordinating atomic persistence operations (appending new log segments or writing full snapshots) via the IStateMachineStorage. It operates asynchronously in a dedicated work loop.
   - Built-in Durable Types: Provides ready-to-use implementations of common state patterns and collections as durable state machines. These types automatically generate log entries for their modifications via the IStateMachineLogWriter interface (provided by the StateMachineManager):
      - IDurableDictionary<K, V>
      - IDurableList<T>
      - IDurableQueue<T>
      - IDurableSet<T>
      - IDurableValue<T> (simple scalar value wrapper)
      - IPersistentState<T> (Implemented by DurableState<T>, providing compatibility with the existing grain persistence programming model for single values, backed by journaling).
      - IDurableTaskCompletionSource<T> (for persisting the outcome of asynchronous operations whose state needs durability).
      - IDurableNothing (a placeholder state machine that performs no operations, useful for marking state machines as retired).
   - DurableGrain Base Class: A convenient base class that automatically handles dependency injection and lifecycle management for the IStateMachineManager, simplifying grain implementation. It provides the WriteStateAsync() method.
   - VolatileStateMachineStorageProvider: An in-memory provider that implements IStateMachineStorage but does not provide cross-process durability. Ideal for testing or scenarios where persistence isn't required.
   - Hosting Extensions: Facilitates easy registration of the core journaling services (.AddStateMachineStorage()) and makes the built-in durable types available via keyed DI.

2. Orleans.Journaling.AzureStorage Project: This library provides a concrete durable storage implementation for the journaling framework.

   - AzureAppendBlobStateMachineStorageProvider: Implements IStateMachineStorageProvider using Azure Append Blobs. Append blobs are a natural fit for storing the sequential log segments efficiently.
   - Append Blob Logic: Handles reading the full log (or latest snapshot + subsequent entries) during recovery, appending new log segments, replacing the log with snapshots, and deleting the blob. It uses Azure's built-in mechanisms for optimistic concurrency (ETags).
   - Configuration: Includes AzureAppendBlobStateMachineStorageOptions for connection details and container configuration, and hosting extensions (.AddAzureAppendBlobStateMachineStorage()) for easy setup.

3. Serialization Layer Improvements:

   - Introduces ArcBufferWriter, an efficient pooled buffer implementation optimized for building the log segments (LogExtentBuilder), enabling fast in-memory buffering and efficient writing to storage backends. Based on atomic reference counting, with 'slices' taking leases.
   - Includes necessary adjustments to core serialization codecs and the deep copying mechanism to support the new buffer types and durable patterns.

## Usage Examples

Using the new journaling persistence involves configuring a storage provider and then injecting and using the durable types within a grain inheriting from DurableGrain.

1. Silo Configuration (Example with Azure Append Blob Storage):

Add necessary NuGet packages: Microsoft.Orleans.Journaling, Microsoft.Orleans.Journaling.AzureStorage.

```
siloBuilder.AddAzureAppendBlobStateMachineStorage(options =>
{
    // Configure Azure connection - using connection string, managed identity, etc
    options.ConfigureBlobServiceClient("YOUR_AZURE_STORAGE_CONNECTION_STRING");
    options.ContainerName = "orleans-grain-journal";
    // Other options like BlobClientOptions can be configured here
});
```

2. Grain Definition and State Injection:

Define your state using the built-in durable types or custom IDurableStateMachine implementations. Inherit from DurableGrain.

```
[GenerateSerializer]
public record class MySimpleState(string Name, int Count);

public interface IMyDurableGrain : IGrainWithStringKey
{
    Task SetInitialState(string name, int initialCount);
    Task UpdateCountAndDictionary(string key, int value);
    Task<(string Name, int Count)> GetSimpleState();
    Task<int> GetDictionaryValue(string key);
    Task<int> GetDictionaryCount();
}

public class MyDurableGrain(
    [FromKeyedServices("simple-state")] IDurableValue<MySimpleState> simpleState,
    [FromKeyedServices("my-dictionary")] IDurableDictionary<string, int> myDictionary)
    : DurableGrain, IMyDurableGrain
{
    public async Task SetInitialState(string name, int initialCount)
    {
        simpleState.Value = new MySimpleState(name, initialCount);
        myDictionary["initial"] = 1;
        await WriteStateAsync();
    }

    public async Task UpdateCountAndDictionary(string key, int value)
    {
        simpleState.Value = simpleState.Value with { Count = simpleState.Value.Count + 1 };
        myDictionary[key] = value;

        // WriteStateAsync persists changes from BOTH _simpleState and _myDictionary atomically
        await WriteStateAsync();
    }

    public Task<(string Name, int Count)> GetSimpleState()
        => Task.FromResult((simpleState.Value.Name, simpleState.Value.Count));

     public Task<int> GetDictionaryValue(string key)
        => Task.FromResult(myDictionary[key]);

    public Task<int> GetDictionaryCount()
        => Task.FromResult(myDictionary.Count);
}
```

## Testing

Comprehensive unit and integration tests are included in the Orleans.Journaling.Tests project. These tests cover:

  - The core StateMachineManager logic, including log recovery and write coordination.
  - Full test coverage for each of the built-in durable types (IDurableDictionary, IDurableList, IDurableQueue, IDurableSet, IDurableValue, DurableState (via IPersistentState), etc.) to ensure their operations are correctly logged and replayed.
  - Validation of state persistence and recovery against both the VolatileStateMachineStorageProvider and the AzureAppendBlobStateMachineStorageProvider, including scenarios with multiple operations, large state, and atomic updates across multiple state machines within a grain.

> Note: As mentioned, this feature is introduced as experimental (ORLEANSEXP005). The APIs, implementation details, and performance characteristics may evolve based on feedback and further development.
