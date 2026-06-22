package com.xgen.mongot.index.query.operators.mql;

import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.points.Point;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public final class GteOperator extends OpenRangeBoundComparisonOperator {

  private final Point value;

  public GteOperator(Point value, RangeBound<? extends Comparable<?>> bounds) {
    super(bounds);
    this.value = value;
  }

  public Point getValue() {
    return this.value;
  }

  @Override
  public Category getCategory() {
    return Category.LOWER_BOUND;
  }

  @Override
  public BsonValue operatorToBson() {
    return this.value.toBson();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder().field(Fields.GTE, Optional.of(this)).build();
  }

  public static GteOperator fromBson(BsonParseContext context, BsonValue bsonValue)
      throws BsonParseException {
    var value = Values.VALUE.getParser().parse(context, bsonValue);
    var bounds = convertToRangeBounds(context, Optional.of(value), Optional.empty(), true, false);
    return new GteOperator(value, bounds);
  }
}
