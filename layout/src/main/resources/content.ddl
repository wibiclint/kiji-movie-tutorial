CREATE TABLE movies
ROW KEY FORMAT HASH PREFIXED(1)
WITH LOCALITY GROUP default (
    MAXVERSIONS = INFINITY,
    TTL = FOREVER,
    INMEMORY = false,
    COMPRESSED WITH GZIP,
    FAMILY info (
        info CLASS org.kiji.tutorial.movies.avro.MovieInfo
    )
), LOCALITY GROUP in_memory (
    MAXVERSIONS = INFINITY,
    TTL = FOREVER,
    INMEMORY = true,
    MAP TYPE FAMILY most_similar CLASS org.kiji.tutorial.movies.avro.SortedSimilarities
);
