package org.kgusarov.elasticsearch.search.aggregations.bucket.geohashclustering;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

/**
 * A {@code geohash_grid} aggregation. Defines multiple buckets, each representing a cell in a geo-grid of a specific
 * precision.
 */
public interface GeoHashClustering extends MultiBucketsAggregation {
    /**
     * A bucket that is associated with a {@code geohash_grid} cell. The key of the bucket is the {@code geohash} of the cell
     */
    public abstract class Bucket extends InternalMultiBucketAggregation.InternalBucket {
        /**
         * @return  The geohash of the cell as a geo point
         */
        public abstract GeoPoint getKeyAsGeoPoint();

        /**
         * @return  A numeric representation of the geohash of the cell
         */
        public abstract Number getKeyAsNumber();

        /**
         * @return  center of cluster
         */
        public abstract GeoPoint getClusterCenter();
    }

    /**
     * @return  The buckets of this aggregation (each bucket representing a geohash grid cell)
     */
    @Override
    List<Bucket> getBuckets();

    Bucket getBucketByKey(Number key);

    Bucket getBucketByKey(GeoPoint key);

    Bucket getBucketByKey(String key);
}

