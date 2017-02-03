package org.kgusarov.elasticsearch.plugin.geo;

import com.google.common.base.MoreObjects;
import org.elasticsearch.common.geo.GeoPoint;

/**
* Author: Konstantin Gusarov
*/
@SuppressWarnings({"NestedAssignment", "NestedMethodCall"})
public class ClusterBounds {
    private double latMin;
    private double latMax;
    private double lonMin;
    private double lonMax;

    private GeoPoint centroid;

    ClusterBounds(final double latMin, final double latMax,
                  final double lonMin, final double lonMax) {

        this.latMin = latMin;
        this.latMax = latMax;
        this.lonMin = lonMin;
        this.lonMax = lonMax;
    }

    ClusterBounds(final GeoPoint geoPoint) {
        latMin = latMax = geoPoint.getLat();
        lonMin = lonMax = geoPoint.getLon();
    }

    void addPoint(final GeoPoint point) {
        latMin = Math.min(point.getLat(), latMin);
        latMax = Math.max(point.getLat(), latMax);
        lonMin = Math.min(point.getLon(), lonMin);
        lonMax = Math.max(point.getLon(), lonMax);

        centroid = null;
    }

    void merge(final ClusterBounds clusterBounds) {
        latMin = Math.min(clusterBounds.latMin, latMin);
        latMax = Math.max(clusterBounds.latMax, latMax);
        lonMin = Math.min(clusterBounds.lonMin, lonMin);
        lonMax = Math.max(clusterBounds.lonMax, lonMax);

        centroid = null;
    }

    public double getLatMin() {
        return latMin;
    }

    public double getLatMax() {
        return latMax;
    }

    public double getLonMin() {
        return lonMin;
    }

    public double getLonMax() {
        return lonMax;
    }

    public GeoPoint getCentroid() {
        if (centroid == null) {
            centroid = new GeoPoint((latMin + latMax) / 2.0, (lonMin + lonMax) / 2.0);
        }

        return centroid;
    }

//    public double size(final DistanceUnit distanceUnit) {
//
//    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("latMin", latMin)
                .add("latMax", latMax)
                .add("lonMin", lonMin)
                .add("lonMax", lonMax)
                .toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ClusterBounds that = (ClusterBounds) o;
        return Double.compare(that.latMax, latMax) == 0 && Double.compare(that.latMin, latMin) == 0
                && Double.compare(that.lonMax, lonMax) == 0 && Double.compare(that.lonMin, lonMin) == 0;
    }

    @Override
    public int hashCode() {
        long temp = Double.doubleToLongBits(latMin);
        int result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(latMax);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(lonMin);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(lonMax);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
