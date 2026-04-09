package com.xgen.mongot.config.provider.community;

import com.mongodb.TagSet;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;
import org.bson.BsonDocument;

public record ReadPreferenceConfig(
    MongoReadPreferenceName readPreference, Optional<List<TagSet>> tagSets)
    implements DocumentEncodable {

  private static class Fields {
    static final Field.Required<MongoReadPreferenceName> READ_PREFERENCE =
        Field.builder("readPreference")
            .enumField(MongoReadPreferenceName.class)
            .asCamelCase()
            .required();

    static final Field.Optional<List<List<Tag>>> TAG_SETS =
        Field.builder("tagSets")
            .listOf(
                Value.builder()
                    .listOf(
                        Value.builder()
                            .classValue(Tag::fromBson)
                            .disallowUnknownFields()
                            .required())
                    .required())
            .optional()
            .noDefault();
  }

  public static ReadPreferenceConfig fromBson(DocumentParser parser) throws BsonParseException {
    MongoReadPreferenceName readPreference = parser.getField(Fields.READ_PREFERENCE).unwrap();
    Optional<List<List<Tag>>> rawTagSets = parser.getField(Fields.TAG_SETS).unwrap();

    Optional<List<TagSet>> tagSets =
        rawTagSets.map(
            sets ->
                sets.stream()
                    .map(
                        tags ->
                            new TagSet(
                                tags.stream()
                                    .map(t -> new com.mongodb.Tag(t.name(), t.value()))
                                    .toList()))
                    .toList());

    if (readPreference == MongoReadPreferenceName.PRIMARY
        && tagSets.isPresent()
        && !tagSets.get().isEmpty()) {
      parser
          .getContext()
          .handleSemanticError("tagSets must be empty or null when readPreference is PRIMARY");
    }

    return new ReadPreferenceConfig(readPreference, tagSets);
  }

  @Override
  public BsonDocument toBson() {
    Optional<List<List<Tag>>> rawTagSets =
        this.tagSets.map(
            sets ->
                sets.stream()
                    .map(
                        tagSet ->
                            StreamSupport.stream(tagSet.spliterator(), false)
                                .map(tag -> new Tag(tag.getName(), tag.getValue()))
                                .toList())
                    .toList());

    return BsonDocumentBuilder.builder()
        .field(Fields.READ_PREFERENCE, this.readPreference)
        .field(Fields.TAG_SETS, rawTagSets)
        .build();
  }

  private record Tag(String name, String value) implements DocumentEncodable {

    private static class Fields {
      static final Field.Required<String> NAME =
          Field.builder("name").stringField().mustNotBeEmpty().required();
      static final Field.Required<String> VALUE =
          Field.builder("value").stringField().mustNotBeEmpty().required();
    }

    static Tag fromBson(DocumentParser parser) throws BsonParseException {
      return new Tag(parser.getField(Fields.NAME).unwrap(), parser.getField(Fields.VALUE).unwrap());
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.NAME, this.name)
          .field(Fields.VALUE, this.value)
          .build();
    }
  }
}
