package org.kiji.tutorial.scoring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.JobConfigurationException;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.shell.api.Client;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.schema.util.Resources;
import org.kiji.scoring.batch.ScoreFunctionJobBuilder;
import org.kiji.tutorial.avro.ItemSimilarityScore;
import org.kiji.tutorial.avro.MovieRating;
import org.kiji.tutorial.avro.MovieRecommendation;
import org.kiji.tutorial.avro.MovieRecommendations;
import org.kiji.tutorial.avro.SortedSimilarities;

public class TestScoring extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestScoring.class);
  private Kiji mKiji;
  private KijiTable mUsersTable;
  private KijiTable mMoviesTable;
  private KijiTableReader mUsersReader;

  private static final Long DONTCARE_TIMESTAMP = 0L;


  /**
   * Set up the user and movie tables for tests.
   *
   * @throws IOException
   */
  @Before
  public void setupTestScoreFunctionJobBuilder() throws IOException {
    mKiji = getKiji();

    Client ddlClient = Client.newInstance(mKiji.getURI());
    // Create the users table.
    ddlClient.executeStream(getClass().getResourceAsStream("/users.ddl"));
    // Create the movies tables.
    ddlClient.executeStream(getClass().getResourceAsStream("/movies.ddl"));
    System.out.println(ddlClient.getLastOutput());
    ddlClient.close();

    mUsersTable = mKiji.openTable("users");
    mMoviesTable = mKiji.openTable("movies");

    // Populate the users table.
    populateUsersTable();
    populateMoviesTable();

    mUsersReader = mUsersTable.openTableReader();
  }

  private void populateUsersTable() throws IOException {
    final KijiTableWriter writer = mUsersTable.openTableWriter();
    try {
      EntityId eid = mUsersTable.getEntityId("0");
      writer.put(eid, "ratings", "0", new MovieRating(0L, 0, DONTCARE_TIMESTAMP));
      writer.put(eid, "ratings", "1", new MovieRating(1L, 5, DONTCARE_TIMESTAMP));
      writer.put(eid, "ratings", "2", new MovieRating(2L, 5, DONTCARE_TIMESTAMP));
    } finally {
      writer.close();
    }
  }

  private void populateMoviesTable() throws IOException {
    // TODO: Test we are limiting to K-most-similar (not already viewed) movies per movie.
    // Recall that M >> K where M = length of SortedSimilarities (the most-similar items) and
    // K = number of most-similar, not-already-viewed-by-the-user movies to consider for each of the
    // user's favorite movies when making a next-movie recommendation.

    final KijiTableWriter writer = mMoviesTable.openTableWriter();
    try {
      // Movie 0 is most similar to movie 10
      writer.put(
          mMoviesTable.getEntityId("0"),
          "most_similar",
          "most_similar",
          new SortedSimilarities(Arrays.<ItemSimilarityScore>asList(
              new ItemSimilarityScore(10L, 1.0)
          )));

      // Movie 1 is most similar to movies 11 and 12.
      writer.put(
          mMoviesTable.getEntityId("1"),
          "most_similar",
          "most_similar",
          new SortedSimilarities(Arrays.<ItemSimilarityScore>asList(
              new ItemSimilarityScore(11L, 0.5),
              new ItemSimilarityScore(12L, 0.4)
          )));

      // Movie 2 is most similar to movies 12 and 13.
      writer.put(
          mMoviesTable.getEntityId("2"),
          "most_similar",
          "most_similar",
          new SortedSimilarities(Arrays.<ItemSimilarityScore>asList(
              new ItemSimilarityScore(12L, 0.3),
              new ItemSimilarityScore(13L, 0.1)
          )));

    } finally {
      writer.close();
    }
  }

  @After
  public void cleanupTestScoreFunctionJobBuilder() throws IOException {
    mUsersReader.close();
    mUsersTable.release();
    mMoviesTable.release();
  }

  @Test
  public void testSimpleJob() throws IOException, InterruptedException, ClassNotFoundException {
    // Set up system properties to point to the correct KVS.
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(
        MovieRecommendationScoreFunction.KEY_VALUE_STORE_TABLE_URI_PARAMETER_KEY,
        mMoviesTable.getURI().toString()
    );

    final KijiMapReduceJob sfJob = ScoreFunctionJobBuilder.create()
        .withConf(getConf())
        .withInputTable(mUsersTable.getURI())
        .withAttachedColumn(new KijiColumnName("recommendations:foo"))
        .withScoreFunctionClass(MovieRecommendationScoreFunction.class)
        .withParameters(parameters)
        .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(mUsersTable.getURI()))
        .build();

    assertTrue(sfJob.run());

    // Check that we see the movies recommended in the correct order.
    KijiRowData rowData = mUsersReader.get(
        mUsersTable.getEntityId("0"),
        KijiDataRequest.create("recommendations", "foo")
    );

    MovieRecommendations recommendations = rowData.getMostRecentValue("recommendations", "foo");
    assertNotNull(recommendations);

    List<MovieRecommendation> recommendationList = recommendations.getRecommendations();
    assertEquals(3, recommendationList.size());
    assertEquals(Long.valueOf(12L), recommendationList.get(0).getShowId());
    assertEquals(Long.valueOf(11L), recommendationList.get(1).getShowId());
    assertEquals(Long.valueOf(13L), recommendationList.get(2).getShowId());

    assertEquals(0.7, recommendationList.get(0).getWeight(), 0.001);
    assertEquals(0.5, recommendationList.get(1).getWeight(), 0.001);
    assertEquals(0.1, recommendationList.get(2).getWeight(), 0.001);

  }

}