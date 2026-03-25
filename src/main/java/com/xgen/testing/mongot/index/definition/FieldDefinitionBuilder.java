package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.definition.AutocompleteFieldDefinition;
import com.xgen.mongot.index.definition.BooleanFieldDefinition;
import com.xgen.mongot.index.definition.DateFacetFieldDefinition;
import com.xgen.mongot.index.definition.DateFieldDefinition;
import com.xgen.mongot.index.definition.DocumentFieldDefinition;
import com.xgen.mongot.index.definition.EmbeddedDocumentsFieldDefinition;
import com.xgen.mongot.index.definition.FieldDefinition;
import com.xgen.mongot.index.definition.GeoFieldDefinition;
import com.xgen.mongot.index.definition.KnnVectorFieldDefinition;
import com.xgen.mongot.index.definition.NumberFacetFieldDefinition;
import com.xgen.mongot.index.definition.NumberFieldDefinition;
import com.xgen.mongot.index.definition.ObjectIdFieldDefinition;
import com.xgen.mongot.index.definition.SearchAutoEmbedFieldDefinition;
import com.xgen.mongot.index.definition.SearchIndexVectorFieldDefinition;
import com.xgen.mongot.index.definition.SortableDateBetaV1FieldDefinition;
import com.xgen.mongot.index.definition.SortableNumberBetaV1FieldDefinition;
import com.xgen.mongot.index.definition.SortableStringBetaV1FieldDefinition;
import com.xgen.mongot.index.definition.StringFacetFieldDefinition;
import com.xgen.mongot.index.definition.StringFieldDefinition;
import com.xgen.mongot.index.definition.TokenFieldDefinition;
import com.xgen.mongot.index.definition.UuidFieldDefinition;
import java.util.Optional;

public class FieldDefinitionBuilder {

  private Optional<AutocompleteFieldDefinition> autocompleteFieldDefinition = Optional.empty();
  private Optional<BooleanFieldDefinition> booleanFieldDefinition = Optional.empty();
  private Optional<DateFieldDefinition> dateFieldDefinition = Optional.empty();
  private Optional<DateFacetFieldDefinition> dateFacetFieldDefinition = Optional.empty();
  private Optional<DocumentFieldDefinition> documentFieldDefinition = Optional.empty();
  private Optional<EmbeddedDocumentsFieldDefinition> embeddedDocumentsFieldDefinition =
      Optional.empty();
  private Optional<GeoFieldDefinition> geoFieldDefinition = Optional.empty();
  private Optional<SearchAutoEmbedFieldDefinition> searchAutoEmbedFieldDefinition =
      Optional.empty();
  private Optional<SearchIndexVectorFieldDefinition> searchIndexVectorFieldDefinition =
      Optional.empty();
  private Optional<KnnVectorFieldDefinition> knnVectorFieldDefinition = Optional.empty();
  private Optional<NumberFieldDefinition> numericFieldDefinition = Optional.empty();
  private Optional<NumberFacetFieldDefinition> numberFacetFieldDefinition = Optional.empty();
  private Optional<ObjectIdFieldDefinition> objectIdFieldDefinition = Optional.empty();
  private Optional<SortableDateBetaV1FieldDefinition> sortableDateBetaV1FieldDefinition =
      Optional.empty();
  private Optional<SortableNumberBetaV1FieldDefinition> sortableNumberBetaV1FieldDefinition =
      Optional.empty();
  private Optional<SortableStringBetaV1FieldDefinition> sortableStringBetaV1FieldDefinition =
      Optional.empty();
  private Optional<StringFieldDefinition> stringFieldDefinition = Optional.empty();
  private Optional<StringFacetFieldDefinition> stringFacetFieldDefinition = Optional.empty();
  private Optional<TokenFieldDefinition> tokenFieldDefinition = Optional.empty();
  private Optional<UuidFieldDefinition> uuidFieldDefinition = Optional.empty();

  public static FieldDefinitionBuilder builder() {
    return new FieldDefinitionBuilder();
  }

  public FieldDefinitionBuilder autocomplete(
      AutocompleteFieldDefinition autocompleteFieldDefinition) {
    this.autocompleteFieldDefinition = Optional.of(autocompleteFieldDefinition);
    return this;
  }

  public FieldDefinitionBuilder bool(BooleanFieldDefinition booleanFieldDefinition) {
    this.booleanFieldDefinition = Optional.of(booleanFieldDefinition);
    return this;
  }

  public FieldDefinitionBuilder date(DateFieldDefinition dateFieldDefinition) {
    this.dateFieldDefinition = Optional.of(dateFieldDefinition);
    return this;
  }

  public FieldDefinitionBuilder dateFacet(DateFacetFieldDefinition dateFacetFieldDefinition) {
    this.dateFacetFieldDefinition = Optional.of(dateFacetFieldDefinition);
    return this;
  }

  public FieldDefinitionBuilder document(DocumentFieldDefinition documentFieldDefinition) {
    this.documentFieldDefinition = Optional.of(documentFieldDefinition);
    return this;
  }

  public FieldDefinitionBuilder embeddedDocuments(
      EmbeddedDocumentsFieldDefinition embeddedDocumentsFieldDefinition) {
    this.embeddedDocumentsFieldDefinition = Optional.of(embeddedDocumentsFieldDefinition);
    return this;
  }

  public FieldDefinitionBuilder geo(GeoFieldDefinition geoFieldDefinition) {
    this.geoFieldDefinition = Optional.of(geoFieldDefinition);
    return this;
  }

  public FieldDefinitionBuilder searchAutoEmbed(
      SearchAutoEmbedFieldDefinition searchAutoEmbedFieldDefinition) {
    this.searchAutoEmbedFieldDefinition = Optional.of(searchAutoEmbedFieldDefinition);
    return this;
  }

  public FieldDefinitionBuilder searchIndexVectorField(
      SearchIndexVectorFieldDefinition searchIndexVectorFieldDefinition) {
    this.searchIndexVectorFieldDefinition = Optional.of(searchIndexVectorFieldDefinition);
    return this;
  }

  public FieldDefinitionBuilder knnVector(KnnVectorFieldDefinition knnVectorFieldDefinition) {
    this.knnVectorFieldDefinition = Optional.of(knnVectorFieldDefinition);
    return this;
  }

  public FieldDefinitionBuilder number(NumberFieldDefinition numericFieldDefinition) {
    this.numericFieldDefinition = Optional.of(numericFieldDefinition);
    return this;
  }

  public FieldDefinitionBuilder numberFacet(NumberFacetFieldDefinition numberFacetFieldDefinition) {
    this.numberFacetFieldDefinition = Optional.of(numberFacetFieldDefinition);
    return this;
  }

  public FieldDefinitionBuilder objectid(ObjectIdFieldDefinition objectIdFieldDefinition) {
    this.objectIdFieldDefinition = Optional.of(objectIdFieldDefinition);
    return this;
  }

  public FieldDefinitionBuilder sortableNumberBetaV1(
      SortableNumberBetaV1FieldDefinition sortableNumberBetaV1FieldDefinition) {
    this.sortableNumberBetaV1FieldDefinition = Optional.of(sortableNumberBetaV1FieldDefinition);
    return this;
  }

  public FieldDefinitionBuilder sortableDateBetaV1(
      SortableDateBetaV1FieldDefinition sortableDateBetaV1FieldDefinition) {
    this.sortableDateBetaV1FieldDefinition = Optional.of(sortableDateBetaV1FieldDefinition);
    return this;
  }

  public FieldDefinitionBuilder sortableStringBetaV1(
      SortableStringBetaV1FieldDefinition sortableStringBetaV1FieldDefinition) {
    this.sortableStringBetaV1FieldDefinition = Optional.of(sortableStringBetaV1FieldDefinition);
    return this;
  }

  public FieldDefinitionBuilder string(StringFieldDefinition stringFieldDefinition) {
    this.stringFieldDefinition = Optional.of(stringFieldDefinition);
    return this;
  }

  public FieldDefinitionBuilder stringFacet(StringFacetFieldDefinition stringFacetFieldDefinition) {
    this.stringFacetFieldDefinition = Optional.of(stringFacetFieldDefinition);
    return this;
  }

  public FieldDefinitionBuilder token(TokenFieldDefinition tokenFieldDefinition) {
    this.tokenFieldDefinition = Optional.of(tokenFieldDefinition);
    return this;
  }

  public FieldDefinitionBuilder uuid(UuidFieldDefinition uuidFieldDefinition) {
    this.uuidFieldDefinition = Optional.of(uuidFieldDefinition);
    return this;
  }

  public FieldDefinition build() {
    return new FieldDefinition(
        this.autocompleteFieldDefinition,
        this.booleanFieldDefinition,
        this.dateFieldDefinition,
        this.dateFacetFieldDefinition,
        this.documentFieldDefinition,
        this.embeddedDocumentsFieldDefinition,
        this.geoFieldDefinition,
        this.searchAutoEmbedFieldDefinition,
        this.searchIndexVectorFieldDefinition,
        this.knnVectorFieldDefinition,
        this.numericFieldDefinition,
        this.numberFacetFieldDefinition,
        this.objectIdFieldDefinition,
        this.sortableDateBetaV1FieldDefinition,
        this.sortableNumberBetaV1FieldDefinition,
        this.sortableStringBetaV1FieldDefinition,
        this.stringFieldDefinition,
        this.stringFacetFieldDefinition,
        this.tokenFieldDefinition,
        this.uuidFieldDefinition);
  }
}
