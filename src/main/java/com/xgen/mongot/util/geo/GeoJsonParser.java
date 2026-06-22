package com.xgen.mongot.util.geo;

import com.mongodb.client.model.geojson.GeoJsonObjectType;
import com.mongodb.client.model.geojson.Geometry;
import com.mongodb.client.model.geojson.GeometryCollection;
import com.mongodb.client.model.geojson.LineString;
import com.mongodb.client.model.geojson.MultiLineString;
import com.mongodb.client.model.geojson.MultiPoint;
import com.mongodb.client.model.geojson.MultiPolygon;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.PolygonCoordinates;
import com.mongodb.client.model.geojson.Position;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.CheckedStream;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.ClassField;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.FieldValidator;
import com.xgen.mongot.util.bson.parser.ListField;
import com.xgen.mongot.util.bson.parser.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class GeoJsonParser {
  private static class Builders {
    private static final Value.Required<List<Double>> COORDINATE_PAIR =
        Value.builder()
            .doubleValue()
            .mustBeFinite()
            .asList()
            .validate(
                l ->
                    l.size() < 2
                        ? Optional.of("at least 2 numbers must be present in a coordinate")
                        : Optional.empty())
            .required();

    static final ClassField.FieldBuilder<Position> POSITION =
        Field.builder("coordinates")
            .classField(GeoJsonParser::parsePosition, GeoJsonParser::positionToBson);

    static final ListField.FieldBuilder<Position> LINE = POSITION.asList().validate(longerThan(2));

    static final ListField.FieldBuilder<List<Position>> POLYGON =
        Builders.POSITION
            .asList()
            /* Each closed ring must have at least 4 coordinates */
            .validate(longerThan(4))
            .validate(Builders::validateClosedRing)
            .asList()
            /*Polygons must have at least one ring present: */
            .mustNotBeEmpty();

    private static FieldValidator<List<Position>> longerThan(int length) {
      return l ->
          l.size() < length
              ? Optional.of(
                  String.format(
                      "at least %s positions must be present (found %s)", length, l.size()))
              : Optional.empty();
    }

    private static Optional<String> validateClosedRing(List<Position> ring) {
      return ring.getFirst().equals(ring.getLast())
          ? Optional.empty()
          : Optional.of("polygon rings must be closed (last position == first position)");
    }
  }

  static class Fields {
    static final Field.Required<GeoJsonObjectType> TYPE =
        Field.builder("type").enumField(GeoJsonObjectType.class).asUpperCamelCase().required();

    static final Field.Required<List<Geometry>> GEOMETRIES =
        Field.builder("geometries")
            .classField(GeoJsonParser::parseGeometry, GeoJsonParser::geometryToBson)
            .allowUnknownFields()
            .asList()
            .required();

    static final Field.Required<Position> POSITION = Builders.POSITION.required();
    static final Field.Required<List<Position>> MULTIPOINT_POSITIONS =
        Builders.POSITION.asList().required();

    static final Field.Required<List<Position>> LINE_POSITIONS = Builders.LINE.required();
    static final Field.Required<List<List<Position>>> MULTILINE_POSITIONS =
        Builders.LINE.asList().required();

    static final Field.Required<List<List<Position>>> POLYGON_POSITIONS =
        Builders.POLYGON.required();
    static final Field.Required<List<List<List<Position>>>> MULTIPOLYGON_POSITIONS =
        Builders.POLYGON.asList().required();
  }

  /** Parses a geo json point. */
  public static Point parsePoint(DocumentParser parser) throws BsonParseException {
    var type = parser.getField(Fields.TYPE).unwrap();
    if (type != GeoJsonObjectType.POINT) {
      parser.getContext().handleSemanticError("Expected type to be Point, got " + type);
    }
    return new Point(parser.getField(Fields.POSITION).unwrap());
  }

  /** Parses a geo json geometry object. */
  public static Geometry parseGeometry(DocumentParser parser) throws BsonParseException {
    var type = parser.getField(Fields.TYPE).unwrap();
    return switch (type) {
      case GEOMETRY_COLLECTION ->
          new GeometryCollection(parser.getField(Fields.GEOMETRIES).unwrap());
      case LINE_STRING -> new LineString(parser.getField(Fields.LINE_POSITIONS).unwrap());
      case MULTI_LINE_STRING ->
          new MultiLineString(parser.getField(Fields.MULTILINE_POSITIONS).unwrap());
      case MULTI_POINT -> new MultiPoint(parser.getField(Fields.MULTIPOINT_POSITIONS).unwrap());
      case MULTI_POLYGON ->
          new MultiPolygon(
              CheckedStream.from(parser.getField(Fields.MULTIPOLYGON_POSITIONS).unwrap())
                  .mapAndCollectChecked(GeoJsonParser::polygonCoordinates));
      case POINT -> new Point(parser.getField(Fields.POSITION).unwrap());
      case POLYGON ->
          new Polygon(polygonCoordinates(parser.getField(Fields.POLYGON_POSITIONS).unwrap()));
    };
  }

  /** Converts a Geometry to a BsonValue. */
  public static BsonDocument geometryToBson(Geometry geometry) {
    BsonDocumentBuilder builder =
        BsonDocumentBuilder.builder().field(Fields.TYPE, geometry.getType());

    return switch (geometry.getType()) {
      case GEOMETRY_COLLECTION -> {
        List<Geometry> geometries =
            new ArrayList<>(((GeometryCollection) geometry).getGeometries());
        yield builder.field(Fields.GEOMETRIES, geometries).build();
      }

      case LINE_STRING ->
          builder.field(Fields.LINE_POSITIONS, ((LineString) geometry).getCoordinates()).build();

      case MULTI_LINE_STRING ->
          builder
              .field(Fields.MULTILINE_POSITIONS, ((MultiLineString) geometry).getCoordinates())
              .build();

      case MULTI_POINT ->
          builder
              .field(Fields.MULTIPOINT_POSITIONS, ((MultiPoint) geometry).getCoordinates())
              .build();

      case MULTI_POLYGON -> {
        MultiPolygon multiPolygon = (MultiPolygon) geometry;
        List<List<List<Position>>> multiPositions =
            multiPolygon.getCoordinates().stream()
                .map(GeoJsonParser::positionsFromPolygonCoordinates)
                .collect(Collectors.toList());
        yield builder.field(Fields.MULTIPOLYGON_POSITIONS, multiPositions).build();
      }

      case POINT -> builder.field(Fields.POSITION, ((Point) geometry).getPosition()).build();

      case POLYGON -> {
        Polygon polygon = (Polygon) geometry;
        List<List<Position>> positions = positionsFromPolygonCoordinates(polygon.getCoordinates());
        yield builder.field(Fields.POLYGON_POSITIONS, positions).build();
      }
    };
  }

  private static PolygonCoordinates polygonCoordinates(List<List<Position>> positions) {
    Check.checkState(positions.size() > 0, "at least one ring present");
    List<Position> envelope = positions.get(0);
    Check.checkState(envelope.size() >= 4, "at least 4 positions per envelope");

    List<List<Position>> holes = positions.subList(1, positions.size());
    @SuppressWarnings("unchecked")
    List<Position>[] holesArray = holes.toArray((List<Position>[]) new ArrayList[0]);
    return new PolygonCoordinates(envelope, holesArray);
  }

  private static Position parsePosition(BsonParseContext context, BsonValue rawValues)
      throws BsonParseException {
    List<Double> positions = Builders.COORDINATE_PAIR.getParser().parse(context, rawValues);
    return new Position(positions);
  }

  private static BsonValue positionToBson(Position position) {
    return Builders.COORDINATE_PAIR.getEncoder().encode(position.getValues());
  }

  private static List<List<Position>> positionsFromPolygonCoordinates(
      PolygonCoordinates coordinates) {
    List<List<Position>> positions = new ArrayList<>();
    positions.add(coordinates.getExterior());
    positions.addAll(coordinates.getHoles());

    return positions;
  }
}
