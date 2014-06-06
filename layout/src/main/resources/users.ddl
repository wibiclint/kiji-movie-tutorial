CREATE TABLE users
ROW KEY FORMAT (account_number LONG)
WITH LOCALITY GROUP default (
    MAXVERSIONS = INFINITY,
    TTL = FOREVER,
    INMEMORY = false,
    COMPRESSED WITH GZIP,
    FAMILY info (
      info CLASS org.kiji.tutorial.movies.avro.Person
    ),
    FAMILY ratings (
      ratings CLASS org.kiji.tutorial.movies.avro.MovieRating
    )
);
