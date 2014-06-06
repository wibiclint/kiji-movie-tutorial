This is a multi-module Maven project that contains the source code for a movie rating and
recommendation service, MovieAdvisor.

Planned features:

- Top-N recommended movies
- Predicted rating for a single movie
- Explanation of why we recommend a movie?

TODOs:
- normalize ratings by item mean or user mean?
- what to use for M?  (M = number of item similarities to keep for each item)
- what to use for K?  (K = number of items similar to the item in question that a user has rated to
  try to predict rating for item in question)
- Note that M should be much greater than K (need to balance memory with accuracy)
- Should be bulk-loading into HFiles (or something else for Cassandra)
