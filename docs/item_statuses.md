## Purpose of this doc
The purpose of this doc is to explain the various item statuses and schema. 

### The 2 main statuses
There are 2 main status fields in Sylos nodes. They are:
Traversal Status
and
Copy Status

Note that only src nodes have copy status, this is explained more later.

### Possible Traversal Status Values

- successful -- the item was successfully traversed
- failed -- the attempt to traverse the item has failed
- pending -- the item has not yet been attempted to traverse (or maybe it was at one point but set to retry traversal since the last review phase)
- not_on_src -- This is exclusive to destination items, if the item only exists on the destination but 'not on the src FS tree' then we don't care because it's a 1 way sync and these are treated as successful but a different kind of successful. 


### Possible Copy Status Values
(Again as a reminder these are only for src items not dst items)

- successful -- the item was successfully copied over to the destination
- failed -- the item was not successfully copied over to the destination
- pending -- the item either does not exist on the destination, or the src copy is newer. 
- excluded_explicit -- the item has been explicitly marked to be excluded from copying over by the user
- excluded_inherited -- a child of an item that was excluded explicitly by the user. (see point just above this)

### Why copy status only exists on src items
Mainly because it doesn't make sense to double store it, we only care about items that are ONLY on the src that haven't been copied over yet. So we only need to store it there. 


## Explanation of path review actions
Please note that user actions during path review phases will vary depending on which review we're on. 
For example, a node that is 'pending retry' in discovery / traversal review means that we need to traverse that item, whereas 'pending' in that stage would mean it's pending to be copied over (copy status pending). It's important to keep these distinctions in mind when knowing which status types to reference to explain API / UI behavior by the engine. 
