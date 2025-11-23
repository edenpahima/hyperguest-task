
My Solution explanation:

The problem in the code is that multiple workers can work on the same record at the same time. This causes one worker to override the other’s operation, and we lose the correct value.

To fix this, we want to allow workers to operate in parallel, but only on different records.



I created an array of messages for each key.

And maintain an array of free keys, representing keys that are available for workers.

We dequeued a message only if there is a free key
When a message is dequeued:

The corresponding key is removed from the free keys array and blocked by the workerId stored in "mapWorkerIdToKey".

When the worker finishes its job:

We release the key back to the free keys array using a map that stores workerId -> key (the key that the worker blocked).

This ensures that workers can process tasks in parallel without interfering with each other.

My suggestions 
Improved Implementation Suggestion:
I suggest enhancing the queue by including an operation merger.

The operation merger would:

Remove redundant operations – for example, delete all operations that occur before a set operation.

Merge additive operations – combine multiple add and subtract operations into a single operation.

This approach would reduce the number of calls to the database and improve overall efficiency.