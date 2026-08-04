package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.index.MetaResults;
import java.util.List;
import java.util.Optional;
import org.bson.BsonValue;

public class CursorResponse {

  private final List<TestSearchResult> testSearchResults;
  private final Optional<BsonValue> metaResultsBson;
  private final Optional<MetaResults> metaResults;

  public CursorResponse(
      List<TestSearchResult> testSearchResults,
      Optional<MetaResults> metaResults,
      Optional<BsonValue> metaResultsBson) {
    this.testSearchResults = testSearchResults;
    this.metaResults = metaResults;
    this.metaResultsBson = metaResultsBson;
  }

  public List<TestSearchResult> getTestResults() {
    return this.testSearchResults;
  }

  public Optional<BsonValue> getMetaResultsBson() {
    return this.metaResultsBson;
  }

  public Optional<MetaResults> getMetaResults() {
    return this.metaResults;
  }
}
