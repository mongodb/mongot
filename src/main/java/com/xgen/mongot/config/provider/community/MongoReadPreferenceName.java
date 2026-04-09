package com.xgen.mongot.config.provider.community;

import com.mongodb.ReadPreference;
import com.mongodb.TagSet;
import java.util.List;
import java.util.Optional;

public enum MongoReadPreferenceName {
  PRIMARY,
  PRIMARY_PREFERRED,
  SECONDARY,
  SECONDARY_PREFERRED,
  NEAREST;

  public ReadPreference asReadPreference() {
    return switch (this) {
      case PRIMARY -> ReadPreference.primary();
      case PRIMARY_PREFERRED -> ReadPreference.primaryPreferred();
      case SECONDARY -> ReadPreference.secondary();
      case SECONDARY_PREFERRED -> ReadPreference.secondaryPreferred();
      case NEAREST -> ReadPreference.nearest();
    };
  }

  public ReadPreference asReadPreference(Optional<List<TagSet>> tagSets) {
    if (tagSets.isEmpty() || tagSets.get().isEmpty()) {
      return asReadPreference();
    }

    return switch (this) {
      case PRIMARY -> ReadPreference.primary();
      case PRIMARY_PREFERRED -> ReadPreference.primaryPreferred(tagSets.get());
      case SECONDARY -> ReadPreference.secondary(tagSets.get());
      case SECONDARY_PREFERRED -> ReadPreference.secondaryPreferred(tagSets.get());
      case NEAREST -> ReadPreference.nearest(tagSets.get());
    };
  }
}
