package org.kgusarov.elasticsearch.search.aggregations.bucket.geohashclustering;

import org.elasticsearch.common.geo.GeoUtils;

public final class GeoClusterUtils {
    private static final int DIVIDER = 256;

    private GeoClusterUtils() {
    }

    public static double getMeterByPixel(final int zoom, final double lat) {
        return (GeoUtils.EARTH_EQUATOR / DIVIDER) * (StrictMath.cos(StrictMath.toRadians(lat)) / StrictMath.pow(2, zoom));
    }
}

