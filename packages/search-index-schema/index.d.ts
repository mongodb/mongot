export type IndexType = 'search' | 'vectorSearch';

/**
 * Feature flags recognized by the package. Each flag selects a non-base schema variant when
 * enabled; an empty set (or only unrecognized flags) selects the base variant.
 */
export type FeatureFlag =
  | 'sortedIndex'
  | 'autoEmbeddingPublicPreview'
  | 'autoEmbeddingPrivatePreview'
  | 'vectorXPrivatePreview';

/**
 * Return the bundled JSON schema for an index type given the set of enabled feature flags.
 *
 * Variants are evaluated in priority order; the first whose required flags are all enabled is
 * returned, falling back to the base variant when no feature-flagged variant matches.
 */
export function getSchema(
  indexType: IndexType,
  enabledFlags?: FeatureFlag[]
): Record<string, unknown>;
