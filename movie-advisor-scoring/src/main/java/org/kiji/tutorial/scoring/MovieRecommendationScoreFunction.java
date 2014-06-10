/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kiji.tutorial.scoring;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.lib.KijiTableKeyValueStore;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowKeyComponents;
import org.kiji.schema.KijiURI;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.FreshenerGetStoresContext;
import org.kiji.scoring.ScoreFunction;
import org.kiji.tutorial.avro.ItemSimilarityScore;
import org.kiji.tutorial.avro.MovieRating;
import org.kiji.tutorial.avro.MovieRecommendation;
import org.kiji.tutorial.avro.MovieRecommendations;
import org.kiji.tutorial.avro.SortedSimilarities;

/**
 * ScoreFunction implementation which recommends songs based on a user's most recently played track
 * and precalculated track-track recommendations.
 */
public class MovieRecommendationScoreFunction extends ScoreFunction<MovieRecommendations> {
  private static Logger LOG = LoggerFactory.getLogger(MovieRecommendationScoreFunction.class);

  private static final String KVS_NAME = "kvs-most-similar-movies";

  /** Parameter key which holds the KijiURI of the table from which recommendations are read. */
  public static final String KEY_VALUE_STORE_TABLE_URI_PARAMETER_KEY =
      "org.kiji.tutorial.scoring.MovieRecommendationScoreFunction.kvstore_table_uri";

  /**
   * Data request for the track play history of the user. This combined with the KeyValueStore is
   * enough to produce a new recommendation.
   */
  private static final KijiDataRequest REQUEST = KijiDataRequest.create("info", "track_plays");

  // One time setup methods ------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores(
      final FreshenerGetStoresContext context
  ) {
    final String tableURI = context.getParameter(KEY_VALUE_STORE_TABLE_URI_PARAMETER_KEY);

    LOG.info("Using table URI " + tableURI);

    final KeyValueStore<?, ?> kvstore = KijiTableKeyValueStore.builder()
        .withTable(KijiURI.newBuilder(tableURI).build())
        .withColumn("most_similar", "most_similar")
        .build();

    final Map<String, KeyValueStore<?, ?>> stores = Maps.newHashMap();
    stores.put(KVS_NAME, kvstore);
    return stores;
  }

  // Per-request methods ---------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest(final FreshenerContext context) throws IOException {
    // Get all of the movies that the user has reviewed.
    return KijiDataRequest.create("ratings");
  }

  /** {@inheritDoc} */
  @Override
  public TimestampedValue<MovieRecommendations> score(
      final KijiRowData dataToScore, final FreshenerContext context
  ) throws IOException {
    // Open the key value store reader from which we can get recommendations.
    final KeyValueStoreReader<KijiRowKeyComponents, SortedSimilarities> mostSimilarMoviesReader =
        context.getStore(KVS_NAME);

    // Get all of the movies that the user has rated above 3.5 stars.
    Set<Long> moviesUserLikes = Sets.newHashSet();
    for (KijiCell<MovieRating> cell : dataToScore.<MovieRating>asIterable("ratings")) {
      MovieRating movieRating = cell.getData();
      if (movieRating.getRating() > 3.5) {
        moviesUserLikes.add(movieRating.getMovieId());
      }

    }

    // Return the recommended song from the top songs.
    return TimestampedValue.create(recommend(moviesUserLikes, mostSimilarMoviesReader));
  }

  static MovieRecommendations recommend(
      final Set<Long> moviesUserLikes,
      final KeyValueStoreReader<KijiRowKeyComponents, SortedSimilarities> mostSimilarMoviesReader
  ) throws IOException {

    // Map from movie IDs to the sum of similarity to that movie versus any that the user likes.
    Map<Long, Double> moviesToScores = Maps.newHashMap();

    for (Long movieId: moviesUserLikes) {
      SortedSimilarities sortedSimilarities = mostSimilarMoviesReader
          .get(KijiRowKeyComponents.fromComponents(movieId.toString()));
      if (null == sortedSimilarities) {
        LOG.info("Could not find any similar movies for movie " + movieId);
        // TODO: Do something smarter if there is a problem generating a recommendation.
        return new MovieRecommendations(Lists.<MovieRecommendation>newArrayList());
      }
      List<ItemSimilarityScore> similarMovies = mostSimilarMoviesReader
          .get(KijiRowKeyComponents.fromComponents(movieId.toString())).getSimilarities();
      for (ItemSimilarityScore itemSimilarityScore : similarMovies) {
        Long similarMovie = itemSimilarityScore.getItem();
        // Don't recommend a movie that the user has already seen!
        if (moviesUserLikes.contains(similarMovie)) {
          continue;
        }
        if (!moviesToScores.containsKey(similarMovie)) {
          moviesToScores.put(similarMovie, 0.0);
        }
        double currentScore = moviesToScores.get(similarMovie);
        moviesToScores.put(similarMovie, currentScore + itemSimilarityScore.getSimilarity());
      }
    }

    // Now we sort by weight and take the top N.
    List<Map.Entry<Long, Double>> moviesAndScores = Lists.newArrayList(moviesToScores.entrySet());
    Collections.sort(moviesAndScores, new Comparator<Map.Entry<Long, Double>>() {
      @Override
      public int compare(Map.Entry<Long, Double> o1, Map.Entry<Long, Double> o2) {
        return o1.getValue().compareTo(o2.getValue());
      }
    });

    int numMoviesToRecommend = moviesAndScores.size() < 10 ? moviesAndScores.size() : 10;
    List<Map.Entry<Long, Double>> moviesToRecommend =
        moviesAndScores.subList(0, numMoviesToRecommend-1);
    List<MovieRecommendation> recommendations = Lists.newArrayList();

    for (Map.Entry<Long, Double> movieAndRating : moviesToRecommend)  {
      recommendations.add(
          new MovieRecommendation(movieAndRating.getKey(), movieAndRating.getValue()));
    }

    return new MovieRecommendations(recommendations);
  }
}
