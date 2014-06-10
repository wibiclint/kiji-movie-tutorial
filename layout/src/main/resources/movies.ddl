CREATE TABLE movies
ROW KEY FORMAT HASH PREFIXED(1)
WITH LOCALITY GROUP default (
    MAXVERSIONS = INFINITY,
    TTL = FOREVER,
    INMEMORY = false,
    COMPRESSED WITH GZIP,
    FAMILY info (
        info CLASS org.kiji.tutorial.avro.MovieInfo
    )
), LOCALITY GROUP in_memory (
    MAXVERSIONS = INFINITY,
    TTL = FOREVER,
    INMEMORY = true,
    FAMILY most_similar (
      most_similar CLASS org.kiji.tutorial.avro.SortedSimilarities
    )
);
