## Note
Please keep in mind that limit offset usage in duckDB will result in O(n) scans to get to the next offset. Same thing for mongoDB as well if we ever used that.
Same for SQLite and likely many other DB technologies. What I mean is, say you have 10K items, you set a limit of 1000, offset 0 for first query.
Then you make the next offset query to get the next 1000 items from 1000-1999, then SQL has to O(n) scan from 0-1000 to get to your offset item. 
This will scale linearly and sucks when your DB sizes get absurdly large and you require very fast timing between queries. 

I would recommend considering implementing an ID OFFSET method for O(1) skips throughout the queries. But this requires stateful tracking and some updating of how the queries actually work. Still, worth thinking about in some places. 

In practice, our queries are generally pretty contained. The only place I'd consider this really is for something like an empty search if the user (god forbid) is trying to flip through 10M items page by page with an empty search (returning everything) lol. But this is unlikely to come up or reflect real user behavior beyond this hypothetical. Still I wanted to write this somewhere so that someone knows, if they ever find this. 
