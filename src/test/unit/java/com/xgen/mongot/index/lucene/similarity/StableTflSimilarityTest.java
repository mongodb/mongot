package com.xgen.mongot.index.lucene.similarity;

import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SmallFloat;
import org.junit.Assert;
import org.junit.Test;

public class StableTflSimilarityTest {

  private static final float SCORE_EPSILON = 1e-7f;

  @Test
  public void scorer_validInput_returnsExpectedScoreAndExplanation() {
    var similarity = StableTflSimilarity.getInstance();
    CollectionStatistics collectionStats =
        new CollectionStatistics("description", 4, 4, 3003, 2000);
    TermStatistics termStats = new TermStatistics(new BytesRef("photosynthesis"), 3, 3);
    Similarity.SimScorer scorer = similarity.scorer(1, collectionStats, termStats);
    int numTerms = 1000;
    long norm = SmallFloat.intToByte4(numTerms);
    float score = scorer.score(1, norm);
    Assert.assertEquals(1.3955811f, score, SCORE_EPSILON);

    Explanation explain = scorer.explain(Explanation.match(1, "freq"), norm);
    Assert.assertEquals(explain.getValue().floatValue(), score, SCORE_EPSILON);

    var explainString =
        """
        1.3955811 = score(freq=1), computed as boost * tr * tf from:
          1.0 = boost
          0.45454544 = tf, computed as freq / (freq + k1)) from:
            1 = freq
            1.2 = k1, term saturation parameter
          3.0702786 = tr, term rarity, computed as log(1 + (1 - p + 0.05) / (p + 0.05)) from:
            0.001049047818598714 = p, probability that the term appears in the doc, \
        computed as 1 - (1 - m * 2 ^ (-c * tl)) ^ dl from:
              0.00781 = m, multiplicative constant to term mismatch probability
              0.917 = c, decaying constant for term length
              14.0 = tl, term length
              984.0 = dl, document length
        """;
    Assert.assertEquals(explainString, explain.toString());
  }
}
