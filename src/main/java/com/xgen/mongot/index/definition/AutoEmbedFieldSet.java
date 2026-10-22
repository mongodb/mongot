package com.xgen.mongot.index.definition;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.util.FieldPath;
import java.util.Map;
import java.util.Optional;

/** Auto-embed declarations keyed by their full mapping-key path. */
public record AutoEmbedFieldSet(ImmutableMap<FieldPath, SearchAutoEmbedFieldDefinition> byPath) {

  public static AutoEmbedFieldSet create(DocumentFieldDefinition mappings) {
    ImmutableMap.Builder<FieldPath, SearchAutoEmbedFieldDefinition> builder =
        ImmutableMap.builder();
    collect(mappings, Optional.empty(), builder);
    // FieldPath equality is based on the rendered dotted path. If a literal dotted mapping key and
    // an equivalent nested path both exist, keep one entry rather than failing construction with a
    // raw duplicate-key exception.
    return new AutoEmbedFieldSet(builder.buildKeepingLast());
  }

  public static FieldPath mappingKeyPath(Optional<FieldPath> prefix, String key) {
    return prefix.map(parent -> parent.newChild(key)).orElseGet(() -> FieldPath.newRoot(key));
  }

  private static void collect(
      DocumentFieldDefinition document,
      Optional<FieldPath> prefix,
      ImmutableMap.Builder<FieldPath, SearchAutoEmbedFieldDefinition> builder) {
    for (Map.Entry<String, FieldDefinition> entry : document.fields().entrySet()) {
      FieldDefinition fieldDefinition = entry.getValue();
      Optional<SearchAutoEmbedFieldDefinition> autoEmbed =
          fieldDefinition.searchAutoEmbedFieldDefinition();
      Optional<DocumentFieldDefinition> nestedDocument = fieldDefinition.documentFieldDefinition();
      if (autoEmbed.isEmpty() && nestedDocument.isEmpty()) {
        // Plain leaf: no path needed.
        continue;
      }
      FieldPath path = mappingKeyPath(prefix, entry.getKey());
      autoEmbed.ifPresent(definition -> builder.put(path, definition));
      nestedDocument.ifPresent(nested -> collect(nested, Optional.of(path), builder));
    }
  }

  public boolean isEmpty() {
    return this.byPath.isEmpty();
  }

  public Optional<SearchAutoEmbedFieldDefinition> get(FieldPath path) {
    return Optional.ofNullable(this.byPath.get(path));
  }

  public ImmutableMap<FieldPath, String> modelNamePerPath() {
    return this.byPath.entrySet().stream()
        .collect(
            ImmutableMap.toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().modelName()));
  }
}
