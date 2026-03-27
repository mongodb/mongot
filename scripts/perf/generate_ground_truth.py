#!/usr/bin/env python3
"""
Generate pre-computed ground truth files for ENN recall perf tests.

Connects to a MongoDB cluster containing the perf test datasets, selects 100
deterministic query vectors per collection (sorted by _id), runs exact vector
search ($vectorSearch with exact: true) for each (index, field, limit) combo,
and writes the results as JSON files under src/resources/perf/ground-truth/.

Usage:
    python3 scripts/perf/generate_ground_truth.py --uri "mongodb+srv://..." --dataset-file "query-amazon-voyage-129GB-vector-search-quantized.yml"

Requires: pymongo
    pip install pymongo
"""

import argparse
import json
import os
import sys
from pathlib import Path

try:
    from pymongo import MongoClient
except ImportError:
    print("pymongo is required. Install with: pip install pymongo", file=sys.stderr)
    sys.exit(1)

NUM_QUERY_VECTORS = 100
OUTPUT_BASE = Path(__file__).resolve().parent.parent.parent / "src" / "resources" / "perf" / "ground-truth"

# Each entry: (database, collection, index_name, vector_field, limit)
# Derived from the 6 YAML evaluation configs.
# Keys are the yml file names, values are arrays of config tuples.
TEST_CONFIGS = {
    "query-amazon-voyage-129GB-vector-search-unquantized.yml": [
        ("sample_vectors", "amazon_ecommerce_voyage_latest_2048_binData", "unquantized", "vector", 10),
    ],
    "query-cohere-14GB-vector-search.yml": [
        ("sample_vectors", "cohere_wikipedia_multilingual_v3", "unquantized", "vector", 10),
    ],
    "query-openai3-large-3072-12GB-vector-search.yml": [
        ("sample_vectors", "openai_3_large_ada_002", "cosine_similarity", "vector-1536", 10),
        ("sample_vectors", "openai_3_large_ada_002", "dotproduct_similarity", "vector-1536", 10),
        ("sample_vectors", "openai_3_large_ada_002", "euclidean_similarity", "vector-1536", 10),
    ],
    "query-sphere-18GB-vector-search.yml": [
        ("sample_vectors", "sphere_1M", "default", "vector", 10),
    ],
    "query-cohere-14GB-vector-search-increased-hnsw-params.yml": [
        ("sample_vectors", "cohere_wikipedia_multilingual_v3", "increase-hnsw-construction-params", "vector", 10),
        ("sample_vectors", "cohere_wikipedia_multilingual_v3", "max-out-hnsw-construction-params", "vector", 10)
    ],
}


def select_query_docs(db, collection_name, field, n=NUM_QUERY_VECTORS):
    """Select n documents deterministically, sorted by _id."""
    coll = db[collection_name]
    docs = list(
        coll.find({field: {"$exists": True}})
        .sort("_id", 1)
        .limit(n)
    )
    if len(docs) < n:
        raise ValueError(f"Only found {len(docs)} docs with field '{field}' "
                         f"in {collection_name} (wanted {n})")
    return docs


def run_exact_vector_search(db, collection_name, index_name, field, query_vector, limit):
    """Run $vectorSearch with exact:true and return ordered results."""
    pipeline = [
        {
            "$vectorSearch": {
                "index": index_name,
                "path": field,
                "queryVector": query_vector,
                "limit": limit,
                "exact": True,
            }
        },
        {
            "$project": {
                "score": {"$meta": "vectorSearchScore"},
            }
        },
    ]
    return list(db[collection_name].aggregate(pipeline))


def generate_ground_truth(client, db_name, collection_name, index_name, field, limit, out_path):
    """Generate ground truth for one (collection, index) pair, writing incrementally to file."""
    db = client[db_name]

    print(f"  Selecting {NUM_QUERY_VECTORS} query docs from {db_name}.{collection_name}...")
    query_docs = select_query_docs(db, collection_name, field)

    # Write the header and start of the JSON structure
    with open(out_path, "w") as f:
        f.write('{\n')
        f.write(f'  "database": {json.dumps(db_name)},\n')
        f.write(f'  "collection": {json.dumps(collection_name)},\n')
        f.write(f'  "index": {json.dumps(index_name)},\n')
        f.write(f'  "field": {json.dumps(field)},\n')
        f.write(f'  "limit": {json.dumps(limit)},\n')
        f.write(f'  "numQueries": {len(query_docs)},\n')
        f.write('  "queries": [\n')

    num_queries_written = 0
    for i, doc in enumerate(query_docs):
        query_vector = doc[field]
        doc_id = doc["_id"]

        results = run_exact_vector_search(
            db, collection_name, index_name, field, query_vector, limit
        )

        result_entries = []
        for r in results:
            result_entries.append({
                "_id": str(r["_id"]),
                "score": r["score"],
            })

        query_entry = {
            "queryDocId": str(doc_id),
            "results": result_entries,
        }

        # Append this query result to the file
        with open(out_path, "a") as f:
            if num_queries_written > 0:
                f.write(',\n')
            # Indent the query entry properly (4 spaces for array items)
            query_json = json.dumps(query_entry, indent=2)
            indented = '\n'.join('    ' + line for line in query_json.split('\n'))
            f.write(indented)

        num_queries_written += 1
        print(f"    Processed {i}th query")

    # Close the JSON structure
    with open(out_path, "a") as f:
        f.write('\n  ]\n')
        f.write('}\n')

    return num_queries_written


def main():
    parser = argparse.ArgumentParser(description="Generate ground truth for ANN recall tests")
    parser.add_argument("--uri", required=True, help="MongoDB connection URI")
    parser.add_argument("--dataset-file", required=True,
                        choices=list(TEST_CONFIGS.keys()),
                        help="The YAML data set file key to generate ground truth for")
    parser.add_argument("--output", default=str(OUTPUT_BASE),
                        help=f"Output directory (default: {OUTPUT_BASE})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be generated without connecting")
    args = parser.parse_args()

    configs = TEST_CONFIGS[args.dataset_file]

    if args.dry_run:
        print(f"Would generate ground truth for {args.dataset_file}:")
        for db, coll, idx, field, limit in configs:
            out_path = Path(args.output) / coll / f"{idx}.json"
            print(f"  {db}.{coll} index={idx} field={field} limit={limit}")
            print(f"    -> {out_path}")
        return

    client = MongoClient(args.uri)
    output_base = Path(args.output)

    print(f"Generating ground truth for: {args.dataset_file}")
    for db_name, collection_name, index_name, field, limit in configs:
        print(f"\nGenerating: {collection_name}/{index_name} (limit={limit})")

        out_dir = output_base / collection_name
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{index_name}.json"

        num_queries = generate_ground_truth(
            client, db_name, collection_name, index_name, field, limit, out_path
        )

        print(f"  Wrote {out_path} ({num_queries} queries)")

    client.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
