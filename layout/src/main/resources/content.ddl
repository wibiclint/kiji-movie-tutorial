CREATE TABLE movies
ROW KEY FORMAT HASH PREFIXED(1)
WITH LOCALITY GROUP default (
    MAXVERSIONS = INFINITY,
    TTL = FOREVER,
    INMEMORY = false,
    COMPRESSED WITH GZIP,
    FAMILY info (
        title "string",
        release_date "long",
        video_release_date "long",
        imdb_url "string"
    ),
    MAP TYPE FAMILY genres "string"
), LOCALITY GROUP in_memory (
    MAXVERSIONS = INFINITY,
    TTL = FOREVER,
    INMEMORY = true,
    MAP TYPE FAMILY most_similar CLASS org.kiji.tutorial.movies.avro.SortedSimilarities
);
