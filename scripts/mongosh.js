load("scripts/address_generator.js")

/**
 * Contains functions which are loaded at startup time when running `make docker.connect`
 */

print("To see information about mongot specific functions, run help()")

function help() {
  print("Mongot specific functions (scripts/mongosh.js):")
  print("  count(collection=fts): Count all documents indexed by Mongot")
  print("  time(): measures execution time of a function")
  print(
      "  compare(runnables, n=1, names=[]): compares average runtime of each runnable over n runs")
  print("  insert(size, collection='fts'): indexes `size` random documents")
  print("  randomVec(dim=768, normalize=true) create random array of doubles")
  print("  Indexes.* - index management commands")
}

function count(collection = 'fts') {
  return db[collection].aggregate([{
    $searchMeta: {
      count: {type: "total"},
      queryString: {defaultPath: "_id", query: "*:*"}
    }
  }]);
}

/**
 * Times a runnable.
 *
 * Example usages:
 *   time(() => {})
 *   time(() => printjson(db.claims.aggregate([$search..., $limit...]).toArray()))
 */
function time(runnable, n = 1) {
  if (n > 1) {
    return compare([runnable], n)
  }
  const start = Date.now();
  // Force fetch if result is cursor
  console.log(runnable())
  const end = Date.now();
  print("Time taken:", end - start, "ms");
}

function compare(runnable_list, n = 1, names = []) {
  const all_timings = Array.from(runnable_list, () => []);
  const default_names = Array.from(runnable_list, (_, index) => index)
  const final_names = names.concat(default_names).slice(0,
      runnable_list.length);

  // Warm-up
  for (const r of runnable_list) {
    time(r);
  }

  for (let i = 0; i < n; ++i) {
    for (let j = 0; j < runnable_list.length; ++j) {
      const s = Date.now();
      time(runnable_list[j]);
      all_timings[j].push(Date.now() - s);
    }
  }

  for (let i = 0; i < all_timings.length; ++i) {
    const timings = all_timings[i];
    const mean = timings.reduce((a, b) => a + b) / n
    const stddev = Math.sqrt(
        timings.map(x => Math.pow(x - mean, 2)).reduce((a, b) => a + b) / (n
            - 1))
    const errorMargin = 1.96 * stddev / Math.sqrt(n)
    console.log(final_names[i] + ": Average time taken: " + mean.toFixed(2) + " ± " +
        errorMargin.toFixed(2) + " ms (95% CI); stddev: " + stddev.toFixed(2));
  }
}

const VecType = Object.freeze({
  /** Unit vectors of type float32. */
  UNIT: 0,
  /** Unnormalized float32 vectors. (Requires COSINE similarity) */
  FLOAT: 1,
  /** INT8 vector with values uniformly distributed in [0, 127] */
  INT8: 2,
  /** Bit vector with values uniformly sampled from {0, 1} */
  BIT: 3,
});

/**
 * Generates a random BsonBinary vector size `dim`.
 *
 * @param dim - the number of dimensions of the vector
 * @param type - one of {UNIT, FLOAT, INT8, BIT} specifying the dtype of the vector.
 * All types support COSINE and EUCLIDEAN similarities, but only `UNIT` supports DOT_PRODUCT.
 * All types produce vectors uniformly distributed in their respective space.
 */
function randomVec(dim = 768, type = VecType.UNIT) {
  switch (type) {
    case VecType.UNIT:
      const v = Float32Array.from({length: dim}, () => Mongot.gaussian());
      const length = Math.sqrt(v.reduce((s, a) => s + (a * a), 0));
      for (let i = 0; i < v.length; ++i) {
        v[i] /= length;
      }
      return Binary.fromFloat32Array(v);
    case VecType.FLOAT:
      return Binary.fromFloat32Array(
          Float32Array.from({length: dim}, () => 2 * Math.random() - 1));
    case VecType.INT8:
      return Binary.fromInt8Array(
          Int8Array.from({length: dim}, () => Mongot.nextInt(-128, 128)));
    case VecType.BIT:
      assert.equal(dim % 8, 0, "Bit vector dim must be multiple of 8")
      const packedBits = Int8Array.from({length: dim / 8},
          () => Mongot.randInt(256))
      return Binary.fromPackedBits(packedBits);
  }
}

/**
 * Inserts `size` number of randomly generated documents into db.fts.
 * Each document will have a string, token, number, and date field. To insert
 * into another collection, see {@link Mongot#generate(size)}
 */
function insert(size, collection = 'fts') {
  let sum = 0;
  while (sum < size - Mongot.MAX_BATCH_SIZE) {
    sum += Mongot.MAX_BATCH_SIZE;
    console.log("Inserting documents", sum.toLocaleString(), 'of',
        size.toLocaleString())
    db[collection].insertMany(Mongot.generate(Mongot.MAX_BATCH_SIZE));
  }
  const rem = size - sum
  console.log("Inserting documents", size.toLocaleString(), 'of',
      size.toLocaleString())
  db[collection].insertMany(Mongot.generate(rem));
  let count = db[collection].countDocuments()
  console.log('Total documents in ', collection, ' collection: ' + count)
}

/** Helper functions that don't need to pollute the global namespace. */
class Mongot {

  static get MAX_BATCH_SIZE() {
    return 250_000;
  }

  /** Select one element uniformly at random from the array. */
  static choose(array) {
    return array[Mongot.randInt(array.length)];
  }

  /**
   * Returns a random integer in range [0, max)
   */
  static randInt(max) {
    return Math.floor(Math.random() * max);
  }

  /**
   * Returns a random integer in range [min, max)
   */
  static nextInt(min, max) {
    return Math.floor(min + Math.random() * (max - min));
  }

  static gaussian(mean = 0, stdev = 1) {
    const u = 1 - Math.random(); // Converting [0,1) to (0,1]
    const v = Math.random();
    const z = Math.sqrt(-2.0 * Math.log(u)) * Math.cos(2.0 * Math.PI * v);
    // Transform to the desired mean and standard deviation:
    return z * stdev + mean;
  }

  /**
   * Returns a random alphanumeric string of length `length`
   */
  static randStr(length) {
    let result = '';
    const characters =
        'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    const charactersLength = characters.length;
    for (let i = 0; i < length; ++i) {
      result += characters.charAt(this.randInt(charactersLength));
    }
    return result;
  }

  static randUUID() {
    return new UUID();
  }

  /**
   * Return a uniformly random Date object in range [lower, upper).
   * @param lower - a Date object for the inclusive lower bound
   * @param upper - a Date object for the exclusive upper bound
   */
  static nextDate(lower, upper) {
    const min = lower.getTime();
    const max = upper.getTime();
    return new Date(Mongot.nextInt(min, max));
  }

  /**
   *  Create an array of `size` randomly generated objects
   */
  static generate(size) {
    if (size > Mongot.MAX_BATCH_SIZE) {
      console.warn("Can't generate more than", Mongot.MAX_BATCH_SIZE,
          "docs per request")
      size = Mongot.MAX_BATCH_SIZE
    }
    let results = [];
    for (let i = 0; i < size; ++i) {
      results.push({
        string: this.randStr(2),
        token: this.randStr(8),
        number: 100 * Math.random(),
        date: Mongot.nextDate(
            ISODate("1970-01-01T00:00:00.000Z"),
            ISODate("2025-01-01T00:00:00.000Z")
        ),
        uuid: new UUID(),
        vector: randomVec(VEC_SIZE, VecType.UNIT),
        boolean: Math.random() < .5,
        stored: this.randInt(1000)
      })
    }
    return results;
  }
}

VEC_SIZE = 768

class Indexes {
  static createIndex(col = null) {
    if (col == null) {
      col = db.getSiblingDB("test").fts
    }

    return col.createSearchIndex({
      "name": "default",
      "mappings": {
        "dynamic": true,
        "fields": {
          "token": {
            "type": "token"
          },
          "date": {
            "type": "date"
          },
          "number": {
            "type": "number"
          },
          "vector": {
            "type": "knnVector",
            "similarity": "euclidean",
            "dimensions": VEC_SIZE
          }
        }
      },
      "storedSource": {
        "include": ["stored"]
      }
    })
  }

  static listIndexes(col = null) {
    if (col == null) {
      col = db.getSiblingDB("test").fts
    }

    return col.aggregate([{"$listSearchIndexes": {}}])
  }

  static dropIndex(col = null) {
    if (col == null) {
      col = db.getSiblingDB("test").fts
    }

    return col.dropSearchIndex("default")
  }

}

