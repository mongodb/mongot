package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.InOperator;
import com.xgen.mongot.index.query.operators.value.BooleanValue;
import com.xgen.mongot.index.query.operators.value.DateValue;
import com.xgen.mongot.index.query.operators.value.NonNullValue;
import com.xgen.mongot.index.query.operators.value.NumericValue;
import com.xgen.mongot.index.query.operators.value.ObjectIdValue;
import com.xgen.mongot.index.query.operators.value.StringValue;
import com.xgen.mongot.index.query.operators.value.UuidValue;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.util.FieldPath;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.bson.types.ObjectId;

public class InOperatorBuilder extends OperatorBuilder<InOperator, InOperatorBuilder> {

  private Optional<List<FieldPath>> path = Optional.empty();
  private Optional<List<NonNullValue>> value = Optional.empty();
  private Optional<List<String>> doesNotAffect = Optional.empty();

  public InOperatorBuilder path(String path) {
    this.path = Optional.of(List.of(FieldPath.parse(path)));
    return this;
  }

  // Allows specifying multiple paths, e.g. ["a", "b", "c"]
  public InOperatorBuilder paths(List<String> paths) {
    this.path = Optional.of(paths.stream().map(FieldPath::parse).toList());
    return this;
  }

  public InOperatorBuilder doesNotAffect(List<String> doesNotAffect) {
    this.doesNotAffect = Optional.of(doesNotAffect);
    return this;
  }

  public InOperatorBuilder doesNotAffect(String doesNotAffect) {
    this.doesNotAffect = Optional.of(List.of(doesNotAffect));
    return this;
  }

  public InOperatorBuilder booleans(List<Boolean> values) {
    this.value = Optional.of(values.stream().map(BooleanValue::new).collect(Collectors.toList()));

    return this;
  }

  public InOperatorBuilder objectIds(List<ObjectId> values) {
    this.value = Optional.of(values.stream().map(ObjectIdValue::new).collect(Collectors.toList()));

    return this;
  }

  public InOperatorBuilder dates(List<Date> values) {
    this.value =
        Optional.of(
            values.stream()
                .map((Date date) -> new DateValue(new DatePoint(date)))
                .collect(Collectors.toList()));

    return this;
  }

  public InOperatorBuilder longs(List<Long> values) {
    this.value =
        Optional.of(
            values.stream()
                .map((Long value) -> new NumericValue(new LongPoint(value)))
                .collect(Collectors.toList()));

    return this;
  }

  public InOperatorBuilder doubles(List<Double> values) {
    this.value =
        Optional.of(
            values.stream()
                .map((Double value) -> new NumericValue(new DoublePoint(value)))
                .collect(Collectors.toList()));

    return this;
  }

  public InOperatorBuilder strings(List<String> values) {
    this.value = Optional.of(values.stream().map(StringValue::new).collect(Collectors.toList()));

    return this;
  }

  public InOperatorBuilder uuidStrings(List<String> values) {
    this.value =
        Optional.of(
            values.stream().map(UUID::fromString).map(UuidValue::new).collect(Collectors.toList()));
    return this;
  }

  @Override
  protected InOperatorBuilder getBuilder() {
    return this;
  }

  @Override
  public InOperator build() {
    return new InOperator(getScore(), this.path.get(), this.value.get(), this.doesNotAffect);
  }
}
