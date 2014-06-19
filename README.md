This is a multi-module Maven project that contains the source code for a movie rating and
recommendation service, MovieAdvisor.

Planned features:

- Top-N recommended movies
- Predicted rating for a single movie
- Explanation of why we recommend a movie?

TODOs
-----

### Back-end code

- Should be bulk-loading into HFiles (or something else for Cassandra)
- Use typed pipes everywhere in Express code and use case classes for pipe types
  - Fix this once we have typed pipes in express
- Possibly implement some batch recommenders in Java to show how to use KijiMR and KijiSchema
- Set up a model repository and scoring server?
  - Likely not right now, these are too early
- Run the scorer in batch to provide initial recommendations for each user?


### Front-end code

- Need a way to create a new user
- Organize routes into different name spaces
- Refactor out code that talks to Kiji as much as possible
- Unit tests!!!!!!!
  - Not sure how to do this with Kiji
    - Can include KijiSchema test JAR and use KijiClientTest (also use separate profile for testing
      in leiningen)
    - Can also use the Clojure `with-redefs` macro to redefine the Kiji interface to do reasonable
      things
- Need to close all of the various table readers and writers after testing


### Recommendation stuff

- Calculate global best movies to use as default recommendations?
- Address cold start problem by seeding each new user with the globally most-popular movies?
- Show users why they were recommended a particular movie
  - Add field for "why" to MovieRecommendation Avro record
  - During scoring, keep track of the movies whose weight contributed to a given recommendation
  - Then add the most significant contributors to the Avro record


### Solr integration

- Personalized search
  - Figure out what genres a given user likes the most
  - Weight search terms by genres
  - Work with MovieLens 10M dataset to use tags



