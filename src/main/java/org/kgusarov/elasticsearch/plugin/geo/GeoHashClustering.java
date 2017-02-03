package org.kgusarov.elasticsearch.plugin.geo;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

public interface GeoHashClustering extends MultiBucketsAggregation {
    public abstract class Bucket extends InternalMultiBucketAggregation.InternalBucket {
        public abstract GeoPoint getKeyAsGeoPoint();
        public abstract Number getKeyAsNumber();
        public abstract GeoPoint getClusterCenter();
        public abstract ClusterBounds getClusterBounds();
    }

    @Override
    List<Bucket> getBuckets();

    Bucket getBucketByKey(Number key);

    Bucket getBucketByKey(GeoPoint key);

    Bucket getBucketByKey(String key);
}

