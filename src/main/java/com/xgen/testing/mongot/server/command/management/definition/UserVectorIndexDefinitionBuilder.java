package com.xgen.testing.mongot.server.command.management.definition;

import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorQuantization;
import com.xgen.mongot.index.definition.VectorSimilarity;
import com.xgen.mongot.server.command.management.definition.common.UserVectorIndexDefinition;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.definition.VectorDataFieldDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorFilterFieldDefinitionBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class UserVectorIndexDefinitionBuilder {
  private List<VectorIndexFieldDefinition> fields = new ArrayList<>();
  private int numPartitions = 1;

  public static UserVectorIndexDefinitionBuilder builder() {
    return new UserVectorIndexDefinitionBuilder();
  }

  public UserVectorIndexDefinitionBuilder setFields(List<VectorIndexFieldDefinition> fields) {
    this.fields = fields;
    return this;
  }

  public UserVectorIndexDefinitionBuilder numPartitions(int numPartitions) {
    this.numPartitions = numPartitions;
    return this;
  }

  public UserVectorIndexDefinitionBuilder addVectorField(
      int numDimensions,
      VectorSimilarity similarity,
      VectorQuantization quantization,
      String path) {
    this.fields.add(
        VectorDataFieldDefinitionBuilder.builder()
            .numDimensions(numDimensions)
            .similarity(similarity)
            .path(FieldPath.parse(path))
            .quantization(quantization)
            .build());
    return this;
  }

  public UserVectorIndexDefinitionBuilder addFilterField(String field) {
    this.fields.add(VectorFilterFieldDefinitionBuilder.atPath(field));
    return this;
  }

  public UserVectorIndexDefinition build() {
    return new UserVectorIndexDefinition(
        this.fields, this.numPartitions, Optional.empty(), Optional.empty());
  }
}
