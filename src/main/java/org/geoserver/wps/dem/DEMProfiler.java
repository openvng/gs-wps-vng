package org.geoserver.wps.dem;

import java.util.logging.Logger;

import org.geoserver.wps.gs.GeoServerProcess;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTS;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.spatialstatistics.core.FeatureTypes;
import org.geotools.process.spatialstatistics.gridcoverage.RasterFunctionalSurface;
import org.geotools.referencing.CRS;
import org.geotools.util.logging.Logging;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.util.ProgressListener;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

@DescribeProcess(title = "profile", description = "DEM profiling by a linestring")
public class DEMProfiler implements GeoServerProcess {
  
  /** The LOGGER. */
  private static final Logger LOGGER = Logging
          .getLogger(DEMProfiler.class);

  @DescribeResult(description = "Geometry collection created by DEM profiling")
  public SimpleFeatureCollection execute(
    @DescribeParameter(name = "dem", description = "Input raster(DEM)") GridCoverage2D inputCoverage,
    @DescribeParameter(name = "input", description = "Input geometry(linestring)") Geometry userGeometry,
    @DescribeParameter(name = "srid", description = "SRID") Integer srid,
    @DescribeParameter(name = "interval", description = "interval", min = 0, max = 1) Double interval,
    ProgressListener monitor) throws ProcessException {

    LOGGER.info("begin profile ------------");
    
    if (interval == null || interval <= 0) {
      interval = userGeometry.getLength() / 20;
    }
    
    CoordinateReferenceSystem rasterCRS = inputCoverage.getCoordinateReferenceSystem();
    CoordinateReferenceSystem geometryCRS = null;
    try {
      geometryCRS = CRS.decode("EPSG:"+srid);
    } catch (Exception e) {
      throw new ProcessException(e);
    }
    
    if (!CRS.equalsIgnoreMetadata(rasterCRS, geometryCRS)) {
      try {
        MathTransform transform = CRS.findMathTransform(geometryCRS, rasterCRS);
        userGeometry = JTS.transform(userGeometry, transform);
      } catch (Exception e) {
        throw new ProcessException(e);
      }
    }
    
    RasterFunctionalSurface process = new RasterFunctionalSurface(inputCoverage);
    Geometry profileLine = process.getProfile(userGeometry, interval);
    
    if (!CRS.equalsIgnoreMetadata(rasterCRS, geometryCRS)) {
      try {
        MathTransform transform = CRS.findMathTransform(rasterCRS, geometryCRS);
        profileLine = JTS.transform(profileLine, transform);
      } catch (Exception e) {
        throw new ProcessException(e);
      }
    }
    
    SimpleFeatureType featureType = FeatureTypes.getDefaultType("profile", Point.class, geometryCRS);
    featureType = FeatureTypes.add(featureType, "distance", Double.class);
    featureType = FeatureTypes.add(featureType, "elevation", Double.class);
    
    ListFeatureCollection result = new ListFeatureCollection(featureType);
    SimpleFeatureBuilder builder = new SimpleFeatureBuilder(featureType);
    
    Coordinate[] coords = profileLine.getCoordinates();
    int id = 0;
    double distance = 0;
    for (Coordinate coord : coords) {
      Coordinate coord2D = new Coordinate(coord.x, coord.y);
      Point curPoint = profileLine.getFactory().createPoint(coord2D);
      
      String fid = featureType.getTypeName() + "." + (++id);
      SimpleFeature newFeature = builder.buildFeature(fid);
      newFeature.setDefaultGeometry(curPoint);
      newFeature.setAttribute("distance", distance);
      newFeature.setAttribute("elevation", coord.z);
      result.add(newFeature);
      distance += interval;
    }
    
    LOGGER.info("end profile ------------");
    return result;
  }
  
}
