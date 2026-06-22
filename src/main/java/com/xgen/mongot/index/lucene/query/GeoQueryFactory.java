package com.xgen.mongot.index.lucene.query;

import com.mongodb.client.model.geojson.Geometry;
import com.mongodb.client.model.geojson.MultiPolygon;
import com.xgen.mongot.index.lucene.field.FieldName;
import com.xgen.mongot.index.lucene.geo.InvalidGeoPosition;
import com.xgen.mongot.index.lucene.geo.LuceneGeoTranslator;
import com.xgen.mongot.index.lucene.query.context.SearchQueryFactoryContext;
import com.xgen.mongot.index.lucene.query.util.BooleanComposer;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.index.query.operators.GeoShapeOperator;
import com.xgen.mongot.index.query.operators.GeoWithinOperator;
import com.xgen.mongot.index.query.shapes.Box;
import com.xgen.mongot.index.query.shapes.Circle;
import com.xgen.mongot.index.query.shapes.GeometryShape;
import com.xgen.mongot.index.query.shapes.Shape;
import com.xgen.mongot.util.CheckedStream;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.geo.GeometryFlattener;
import com.xgen.mongot.util.geo.LeafGeometry;
import java.util.List;
import java.util.stream.Stream;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;

class GeoQueryFactory {

  private final SearchQueryFactoryContext queryFactoryContext;

  GeoQueryFactory(SearchQueryFactoryContext queryFactoryContext) {
    this.queryFactoryContext = queryFactoryContext;
  }

  Query geoWithin(GeoWithinOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    return BooleanComposer.StreamUtils.from(operator.paths())
        .mapChecked(
            p -> geoWithinSingle(p, operator, singleQueryContext), BooleanClause.Occur.SHOULD);
  }

  private Query geoWithinSingle(
      FieldPath path, GeoWithinOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    InvalidQueryException.validate(
        this.queryFactoryContext
            .getQueryTimeMappingChecks()
            .indexedAsGeoPoint(path, singleQueryContext.getEmbeddedRoot()),
        "geoWithin requires path '%s' to be indexed as 'geo'",
        path);

    String field =
        FieldName.TypeField.GEO_POINT.getLuceneFieldName(
            path, singleQueryContext.getEmbeddedRoot());
    try {
      Shape shape = operator.shape();
      return switch (shape) {
        case Box box -> withinBox(field, box);
        case Circle circle -> withinCircle(field, circle);
        case GeometryShape geometryShape -> withinPolygon(field, geometryShape);
      };
    } catch (InvalidGeoPosition e) {
      throw new InvalidQueryException(e.getMessage());
    }
  }

  private Query withinCircle(String field, Circle circle) throws InvalidGeoPosition {
    org.apache.lucene.geo.Point point = LuceneGeoTranslator.point(circle.center());
    return LatLonPoint.newDistanceQuery(
        field, point.getLat(), point.getLon(), circle.radiusMeters());
  }

  private Query withinBox(String field, Box box) throws InvalidGeoPosition, InvalidQueryException {
    org.apache.lucene.geo.Point bottomLeft = LuceneGeoTranslator.point(box.bottomLeft());
    org.apache.lucene.geo.Point topRight = LuceneGeoTranslator.point(box.topRight());
    InvalidQueryException.validate(
        bottomLeft.getLat() < topRight.getLat(),
        "bottomLeft.latitude must be smaller than topRight.latitude");
    InvalidQueryException.validate(
        bottomLeft.getLon() < topRight.getLon(),
        "bottomLeft.longitude must be smaller than topRight.longitude");
    return LatLonPoint.newBoxQuery(
        field, bottomLeft.getLat(), topRight.getLat(), bottomLeft.getLon(), topRight.getLon());
  }

  private Query withinPolygon(String field, GeometryShape geometryShape) throws InvalidGeoPosition {
    Geometry geometry = geometryShape.geometry();
    // this is an invariant of the operator:
    return switch (geometry.getType()) {
      case POLYGON -> {
        Polygon polygon =
            LuceneGeoTranslator.polygon((com.mongodb.client.model.geojson.Polygon) geometry);
        yield LatLonPoint.newPolygonQuery(field, polygon);
      }
      case MULTI_POLYGON -> {
        List<Polygon> polygons = LuceneGeoTranslator.multiPolygon((MultiPolygon) geometry);
        yield LatLonPoint.newPolygonQuery(field, polygons.toArray(Polygon[]::new));
      }
      default ->
          throw new IllegalArgumentException(
              String.format("Shape %s not supported for geoWithin query.", geometry.getType()));
    };
  }

  public Query geoShape(GeoShapeOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    return BooleanComposer.StreamUtils.from(operator.paths())
        .mapChecked(
            p -> geoShapeSingle(p, operator, singleQueryContext), BooleanClause.Occur.SHOULD);
  }

  private Query geoShapeSingle(
      FieldPath path, GeoShapeOperator operator, SingleQueryContext singleQueryContext)
      throws InvalidQueryException {
    InvalidQueryException.validate(
        this.queryFactoryContext
            .getQueryTimeMappingChecks()
            .indexedAsGeoShape(path, singleQueryContext.getEmbeddedRoot()),
        "geoShape requires path '%s' to be indexed as 'geo' with indexShapes=true",
        path);

    String field =
        FieldName.TypeField.GEO_SHAPE.getLuceneFieldName(
            path, singleQueryContext.getEmbeddedRoot());
    try {
      Geometry shape = operator.geometryShape().geometry();
      return geoShapeGeometry(field, operator.relation(), shape);
    } catch (InvalidGeoPosition e) {
      throw new InvalidQueryException(e.getMessage());
    }
  }

  private Query geoShapeGeometry(
      String field, GeoShapeOperator.Relation relation, Geometry geometry)
      throws InvalidGeoPosition {
    ShapeField.QueryRelation luceneRelation = luceneRelation(relation);

    Stream<LeafGeometry> geometries = GeometryFlattener.flatten(geometry);

    LatLonGeometry[] luceneGeometries =
        CheckedStream.from(geometries)
            .mapAndCollectChecked(LuceneGeoTranslator::translate)
            .toArray(new LatLonGeometry[0]);

    return LatLonShape.newGeometryQuery(field, luceneRelation, luceneGeometries);
  }

  private ShapeField.QueryRelation luceneRelation(GeoShapeOperator.Relation relation) {
    return switch (relation) {
      case CONTAINS -> ShapeField.QueryRelation.CONTAINS;
      case DISJOINT -> ShapeField.QueryRelation.DISJOINT;
      case INTERSECTS -> ShapeField.QueryRelation.INTERSECTS;
      case WITHIN -> ShapeField.QueryRelation.WITHIN;
    };
  }
}
