package com.xgen.mongot.util.bson;

/**
 * A checked {@link Exception} that's thrown when an error is encountered during conversion from one
 * POJO into another. For example, when a subfield in the source type has an undefined mapping to
 * the destination type.
 */
public class TypeConversionException extends Exception {

  public TypeConversionException(String message) {
    super(message);
  }

  public TypeConversionException(String message, Exception cause) {
    super(message, cause);
  }
}
