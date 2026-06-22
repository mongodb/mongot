package com.xgen.mongot.index.lucene;

import com.xgen.mongot.index.lucene.config.LuceneConfig;
import com.xgen.testing.mongot.index.lucene.LuceneConfigBuilder;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.lucene.search.IndexSearcher;
import org.junit.Assert;
import org.junit.Test;

public class LuceneGlobalSettingsTest {
  @Test
  public void testSettingsAreApplied() {
    LuceneConfig luceneConfig =
        LuceneConfigBuilder.builder().tempDataPath().disableMaxClauses(true).build();

    int saveValue = IndexSearcher.getMaxClauseCount();
    try {
      LuceneGlobalSettings.apply(luceneConfig);
      Assert.assertEquals(Integer.MAX_VALUE, IndexSearcher.getMaxClauseCount());
    } finally {
      IndexSearcher.setMaxClauseCount(saveValue);
      Assert.assertEquals(saveValue, IndexSearcher.getMaxClauseCount());
    }
  }

  @Test
  public void testDisableFalse_maxClauseLimitSet_usesConfigValue() {
    LuceneConfig config = configWithMaxClauseLimit(5_000);

    int saveValue = IndexSearcher.getMaxClauseCount();
    try {
      LuceneGlobalSettings.apply(config);
      Assert.assertEquals(5_000, IndexSearcher.getMaxClauseCount());
    } finally {
      IndexSearcher.setMaxClauseCount(saveValue);
    }
  }

  @Test
  public void testDisableFalse_maxClauseLimitNotSet_preservesLuceneDefault() {
    LuceneConfig config = configWithMaxClauseLimitAndDisable(Optional.empty(), Optional.of(false));

    int saveValue = IndexSearcher.getMaxClauseCount();
    try {
      LuceneGlobalSettings.apply(config);
      Assert.assertEquals(saveValue, IndexSearcher.getMaxClauseCount());
    } finally {
      IndexSearcher.setMaxClauseCount(saveValue);
    }
  }

  @Test
  public void testDisableTrue_maxClauseLimitSet_disableWins() {
    LuceneConfig config =
        configWithMaxClauseLimitAndDisable(Optional.of(50_000), Optional.of(true));

    int saveValue = IndexSearcher.getMaxClauseCount();
    try {
      LuceneGlobalSettings.apply(config);
      Assert.assertEquals(Integer.MAX_VALUE, IndexSearcher.getMaxClauseCount());
    } finally {
      IndexSearcher.setMaxClauseCount(saveValue);
    }
  }

  @Test
  public void testNeitherFlagSet_usesLuceneDefault1024() {
    LuceneConfig config = configWithMaxClauseLimitAndDisable(Optional.empty(), Optional.empty());

    int saveValue = IndexSearcher.getMaxClauseCount();
    try {
      IndexSearcher.setMaxClauseCount(1024);
      LuceneGlobalSettings.apply(config);
      Assert.assertEquals(1024, IndexSearcher.getMaxClauseCount());
    } finally {
      IndexSearcher.setMaxClauseCount(saveValue);
    }
  }

  @Test
  public void testDisableTrue_maxClauseLimitNotSet_limitIsIntegerMax() {
    LuceneConfig config = configWithMaxClauseLimitAndDisable(Optional.empty(), Optional.of(true));

    int saveValue = IndexSearcher.getMaxClauseCount();
    try {
      LuceneGlobalSettings.apply(config);
      Assert.assertEquals(Integer.MAX_VALUE, IndexSearcher.getMaxClauseCount());
    } finally {
      IndexSearcher.setMaxClauseCount(saveValue);
    }
  }

  private static LuceneConfig configWithMaxClauseLimit(int maxClauseLimit) {
    return configWithMaxClauseLimitAndDisable(Optional.of(maxClauseLimit), Optional.of(false));
  }

  private static LuceneConfig configWithMaxClauseLimitAndDisable(
      Optional<Integer> maxClauseLimit, Optional<Boolean> disableMaxClauseLimit) {
    return LuceneConfig.create(
        Path.of("temp"),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        disableMaxClauseLimit,
        maxClauseLimit,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }
}
