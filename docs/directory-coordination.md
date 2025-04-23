# Distributed Grain Directory (#9103)

The distributed grain directory implementation has the following attributes:

- Strong consistency: the new directory maintains strong consistency for all silos which are active members of the cluster. The existing directory is eventually consistent, which means that there can be duplicate grain activations for a short period during cluster membership changes, while the cluster is coalescing.
- Reliable: with the existing directory, when a silo is removed from the cluster (eg, due to a crash or an upgrade), any grains registered to that silo must be terminated. This causes a significant amount of disruption during upgrades, to the point where some teams prefer to perform blue/green upgrades instead of rolling upgrades. The new directory does not suffer from this issue: when cluster membership changes, silos hand-off the registrations which they owned to the new owners. If a silo terminates before hand-off can be completed, recovery is performed: each silo in the cluster is asked for its currently active grains and these are re-registered before continuing.
- Balanced: the existing directory uses a consistent hash ring to assign ownership for key ranges to silos. The new directory also uses a consistent hash ring, but instead of assigning each silo one section of the ring, it assigns each silo a configurable number of sections on the ring, defaulting to 30. This has a couple of benefits. First, it probabilistically improves balance across hosts to within a few percent of each other. Second, it spreads the load of range hand-off between more silos. Giving each silo a single section on the hash ring would result in hand-off always occurring between a pair of hosts.

## Design

The directory is distributed across all silos in the cluster by partitioning the key space into range using consistent hashing where each silo owns some pre-configured number of ring ranges, defaulting to 30 ranges per silo. A hash function is used to associate GrainIds with a point on the ring. The silo which owns that point on the ring is responsible for serving any requests for that grain.

Ring ranges are assigned to replicas deterministically based on the current cluster membership. We refer to consistent snapshots of membership as views, and each view has a unique number which is used to identify and order views. A new view is created whenever a silo in the cluster changes state (eg, between Joining, Active, ShuttingDown, and Dead), and all non-faulty silos learn of view changes quickly through a combination of gossip and polling of the membership service.

The image below shows a representation of a ring with 3 silos (A, B, & C), each owning 4 ranges on the ring. The grain user/terry hashes to 0x4000_000, which is owned by Silo B.

![alt directory-coordination-1](assets\directory-coordination-1.png)

The directory operates in two modes: normal operation where a fixed set of processes communicate in the absence of failure, and view change, which punctuates periods of normal operation to transfer of state and control from the processes in the preceding view to processes in the current view.

![alt directory-coordination-1](assets\directory-coordination-2.png)

Once state and control has been transferred from the previous view, normal operation resumes. This coordinated state and control transfer is performed on a range-by-range basis and some ranges may resume normal operation before others. For partitions which do not change owners during a view change, normal operation never stops. For partitions which do change ownership during a view change, service is suspended when the previous owner learns of the view change and resumed when current owner successfully transfers state and control.

When a view change begins, any ring ranges which were previously owned and are now no-longer owned are sealed, preventing further modifications by the previous owner, and a snapshot is created and retained in-memory. The snapshot is transferred to the new range owners and is deleted once the new owners acknowledge that it has been retrieved or if they are evicted from the cluster.

The image below depicts a view change, where Silo B is shutting down and Silo C must transfer state & control from Silo B for the ranges which it owned. Silo B will not complete shutting down until Silo C acknowledges that it has completed the transfer.

![alt directory-coordination-1](assets\directory-coordination-3.png)

After the transfer has completed, the ring looks like so:

![alt directory-coordination-1](assets\directory-coordination-4.png)

Failures are handled by performing recovery. Partition owners perform recovery by requesting the set of registered grain activations belonging to that partition from all active silos in the cluster. Requests are not served until recovery completes. As an optimization, the new partition owner can serve lookup requests for grain registrations which it has already recovered.

To prevent requests from being served during a view change, range locks are acquired during view change, respected by each request, and released when view change completes.

All requests and responses include the current membership version. If a client or replica receives a message with a higher membership version, they refresh membership to at least that version before continuing. After refreshing their view, clients retry misdirected requests (requests targeting the wrong replica for a given point on the ring), sending them to the owner for the current view. Replicas reject misdirected requests.

Range partitioning is more complicated than having a fixed number of fixed-size partitions, but it has nice properties such as elastic scaling (adding more hosts increases system capacity), good balancing, minimal data movement during scaling events, and the partition mapping is derived directly from cluster membership with no other state (such as partition assignments) required.

### Enabling DistributedGrainDirectory

For now, the new directory is opt-in, so you will need to use the following code to enable it on your silos:

```
#pragma warning disable ORLEANSEXP002 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.
siloBuilder.AddDistributedGrainDirectory();
#pragma warning restore ORLEANSEXP002 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.
```

If performing a rolling upgrade, you will experience unavailability of the directory until all hosts have the new directory

### Does this allow strong single-activation guarantees?

This directory guarantees that a grain will never be concurrently active on more than one silo which is a member of the cluster but it is possible for a grain to still be active on a silo which was evicted from the cluster but is still operating (eg, it's very slow or has experienced a network partition and has not refreshed membership yet). 
