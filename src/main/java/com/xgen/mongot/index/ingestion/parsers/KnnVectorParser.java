package com.xgen.mongot.index.ingestion.parsers;

import com.xgen.mongot.util.bson.FloatVector;
import com.xgen.mongot.util.bson.NativeVectorParser;
import com.xgen.mongot.util.bson.Vector;
import java.util.Optional;
import org.bson.BsonReader;

public class KnnVectorParser {

  /**
   * Iterates through the array and performs narrowing primitive conversion to each element. Note
   * that according to the Java Language Specification, this conversion can lose precision and
   * range, result in a float zero from a nonzero double and a float infinity from a finite double:
   * https://docs.oracle.com/javase/specs/jls/se8/html/jls-5.html#jls-5.1.3.
   */
  public static Optional<Vector> parse(BsonReader reader) {
    return NativeVectorParser.parse(reader)
        .map(floats -> Vector.fromFloats(floats, FloatVector.OriginalType.NATIVE));
  }
}
