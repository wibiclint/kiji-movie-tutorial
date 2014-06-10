CREATE TABLE users
ROW KEY FORMAT HASH PREFIXED(1)
WITH LOCALITY GROUP default (
    MAXVERSIONS = INFINITY,
    TTL = FOREVER,
    INMEMORY = false,
    COMPRESSED WITH GZIP,
    FAMILY info (
      info CLASS org.kiji.tutorial.avro.Person
    ),
    MAP TYPE FAMILY ratings WITH SCHEMA CLASS org.kiji.tutorial.avro.MovieRating
      WITH DESCRIPTION 'Map from movie IDs to ratings',
    MAP TYPE FAMILY recommendations WITH SCHEMA CLASS org.kiji.tutorial.avro.MovieRecommendations
      WITH DESCRIPTION 'Map from scoring functions to movie recommendations'
);
