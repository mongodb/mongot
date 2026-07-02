package com.xgen.mongot.index.query;

import com.xgen.mongot.index.query.counts.Count;
import com.xgen.mongot.index.query.highlights.UnresolvedHighlight;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.index.query.sort.SortSpec;
import com.xgen.mongot.util.Check;
import java.util.Optional;
import org.bson.BsonDocument;

public record OperatorQuery(
    Operator operator,
    String index,
    Count count,
    Optional<UnresolvedHighlight> highlight,
    Optional<Pagination> pagination,
    boolean returnStoredSource,
    boolean scoreDetails,
    boolean concurrent,
    Optional<SortSpec> rawSortSpec,
    Optional<Tracking> tracking,
    Optional<ReturnScope> returnScope)
    implements SearchQuery {

  public OperatorQuery {
    Check.argNotEmpty(index, "index");
    Check.argNotNull(operator, "operator");
  }

  public OperatorQuery withOperator(Operator newOperator) {
    return new OperatorQuery(
        newOperator,
        this.index,
        this.count,
        this.highlight,
        this.pagination,
        this.returnStoredSource,
        this.scoreDetails,
        this.concurrent,
        this.rawSortSpec,
        this.tracking,
        this.returnScope);
  }

  @Override
  public BsonDocument queryToBson() {
    return this.operator.toBson();
  }
}
