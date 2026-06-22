package com.xgen.mongot.index.lucene.geo;

import com.google.common.primitives.Doubles;
import com.mongodb.client.model.geojson.LineString;
import com.mongodb.client.model.geojson.MultiPolygon;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.PolygonCoordinates;
import com.mongodb.client.model.geojson.Position;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.CheckedStream;
import com.xgen.mongot.util.geo.LeafGeometry;
import java.util.List;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Line;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Translates GeoJson to lucene geometries. */
public class LuceneGeoTranslator {
  private static final Logger logger = LoggerFactory.getLogger(LuceneGeoTranslator.class);

  /** Useful when lucene accepts any LatLonGeometry. */
  public static LatLonGeometry translate(LeafGeometry leafGeometry) throws InvalidGeoPosition {
    return switch (leafGeometry.getType()) {
      case LINE_STRING -> line(leafGeometry.asLineString());
      case POINT -> point(leafGeometry.asPoint());
      case POLYGON -> polygon(leafGeometry.asPolygon());
      default -> {
        logger.error("Unexpected LeafGeometry type: {}", leafGeometry.getType());
        yield Check.unreachable("Unexpected LeafGeometry type");
      }
    };
  }

  public static org.apache.lucene.geo.Point point(Point point) throws InvalidGeoPosition {
    Position coordinates = point.getCoordinates();
    return new org.apache.lucene.geo.Point(lat(coordinates), lon(coordinates));
  }

  public static Line line(LineString line) throws InvalidGeoPosition {
    Unzipped positions = Unzipped.positions(line.getCoordinates());
    return new Line(positions.latitude, positions.longitude);
  }

  public static List<org.apache.lucene.geo.Polygon> multiPolygon(MultiPolygon multiPolygon)
      throws InvalidGeoPosition {
    return CheckedStream.from(multiPolygon.getCoordinates())
        .mapAndCollectChecked(LuceneGeoTranslator::polygonCoordinates);
  }

  public static org.apache.lucene.geo.Polygon polygon(Polygon polygon) throws InvalidGeoPosition {
    return polygonCoordinates(polygon.getCoordinates());
  }

  private static org.apache.lucene.geo.Polygon polygonCoordinates(PolygonCoordinates coordinates)
      throws InvalidGeoPosition {
    org.apache.lucene.geo.Polygon[] holes =
        CheckedStream.from(coordinates.getHoles())
            .mapAndCollectChecked(Unzipped::positions)
            .stream()
            .map(cords -> new org.apache.lucene.geo.Polygon(cords.latitude, cords.longitude))
            .toArray(org.apache.lucene.geo.Polygon[]::new);

    Unzipped envelope = Unzipped.positions(coordinates.getExterior());
    // This constructor may throw unchecked exceptions for various validations, however, the bson
    // geometries enforce those too, so it is safe.
    return new org.apache.lucene.geo.Polygon(envelope.latitude, envelope.longitude, holes);
  }

  private static class Unzipped {
    final double[] longitude;
    final double[] latitude;

    Unzipped(double[] longitudes, double[] latitudes) {
      this.longitude = longitudes;
      this.latitude = latitudes;
    }

    static Unzipped positions(List<Position> coordinates) throws InvalidGeoPosition {
      double[] longitude =
          Doubles.toArray(
              CheckedStream.from(coordinates).mapAndCollectChecked(LuceneGeoTranslator::lon));
      double[] latitude =
          Doubles.toArray(
              CheckedStream.from(coordinates).mapAndCollectChecked(LuceneGeoTranslator::lat));
      return new Unzipped(longitude, latitude);
    }
  }

  private static double lat(Position position) throws InvalidGeoPosition {
    // codec asserts that a position has two coordinates
    // The first two elements are longitude and latitude respectively.
    double latitude = position.getValues().get(1);

    try {
      GeoUtils.checkLatitude(latitude);
    } catch (IllegalArgumentException e) {
      throw new InvalidGeoPosition(e.getMessage());
    }
    return latitude;
  }

  private static double lon(Position position) throws InvalidGeoPosition {
    double longitude = position.getValues().get(0);

    try {
      GeoUtils.checkLongitude(longitude);
    } catch (IllegalArgumentException e) {
      throw new InvalidGeoPosition(e.getMessage());
    }
    return longitude;
  }
}
