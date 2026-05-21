# @mongodb-js/search-index-schema

MongoDB Atlas Search Index JSON schemas.

This package provides JSON Schema definitions for MongoDB Atlas Search Indexes, including full-text search indexes and vector search indexes.

## Installation

```bash
npm install @mongodb-js/search-index-schema
```

## Package Structure

- **`schema/`** - source JSON schema files
- **`output/`** - bundled JSON schema files generated from source schemas with all `$ref` pointers resolved

## Schema Files

The package includes schemas for different search index types:

### Search Indexes

- `search/index.json` - Search index schema for use in JSON editors, which excludes metadata such as `name`, `database`, and `collectionName`

### Vector Search Indexes

- `vectorSearch/index.json` - Vector search index schema for use in JSON editors, which excludes metadata such as `name`, `database`, and `collectionName`

## License

Apache-2.0

## Links

- [MongoDB Atlas Search Documentation](https://www.mongodb.com/docs/atlas/atlas-search/)
- [GitHub Repository](https://github.com/mongodb/mongot)
- [Issues](https://github.com/mongodb/mongot/issues)
