package org.kgusarov.elasticsearch.search.aggregations.bucket.geohashclustering;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"SerializableStoresNonSerializable", "NestedMethodCall"})
public class GeoHashClusteringAggregator extends BucketsAggregator {
    private static final int INITIAL_CAPACITY = 50;

    private final ValuesSource.GeoPoint valuesSource;
    private final LongHash bucketOrds;
    private final LongObjectPagedHashMap<ClusterCollector> clusterCollectors;
    private final int zoom;
    private final int distance;

    @SuppressWarnings({"NestedAssignment", "NestedMethodCall"})
    public static class ClusterCollector {
        private double latMin;
        private double latMax;
        private double lonMin;
        private double lonMax;

        public ClusterCollector(final GeoPoint geoPoint) {
            latMin = latMax = geoPoint.getLat();
            lonMin = lonMax = geoPoint.getLon();
        }

        public void addPoint(final GeoPoint point) {
            latMin = Math.min(point.getLat(), latMin);
            latMax = Math.max(point.getLat(), latMax);
            lonMin = Math.min(point.getLon(), lonMin);
            lonMax = Math.max(point.getLon(), lonMax);
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
    }

    public GeoHashClusteringAggregator(final String name, final AggregatorFactories factories, final ValuesSource.GeoPoint valuesSource,
                                       final AggregationContext aggregationContext, final Aggregator parent,
                                       final int zoom, final int distance, final List<PipelineAggregator> pipelineAggregators,
                                       final Map<String, Object> metaData) throws IOException {

        super(name, factories, aggregationContext, parent, pipelineAggregators, metaData);

        this.valuesSource = valuesSource;
        bucketOrds = new LongHash(INITIAL_CAPACITY, aggregationContext.bigArrays());
        clusterCollectors = new LongObjectPagedHashMap<>(INITIAL_CAPACITY, aggregationContext.bigArrays());
        this.zoom = zoom;
        this.distance = distance;
    }

    // private impl that stores a bucket ord. This allows for computing the aggregations lazily.
    static class OrdinalBucket extends InternalGeoHashClustering.Bucket {
        OrdinalBucket() {
            super(0, null, 0, null);
        }
    }

    @Override
    public InternalGeoHashClustering buildAggregation(final long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        final List<InternalGeoHashClustering.Bucket> res = new ArrayList<>();

        for (long i = 0; i < bucketOrds.size(); i++) {
            final long clusterHash = bucketOrds.get(i);

            final ClusterCollector coll = clusterCollectors.get(clusterHash);
            final InternalGeoHashClustering.Bucket bucket = new OrdinalBucket();

            bucket.docCount = bucketDocCount(i);
            bucket.aggregations = bucketAggregations(i);
            bucket.geohashAsLong = clusterHash;
            bucket.geohashesList.add(clusterHash);

            bucket.centroid = new GeoPoint((coll.getLatMin() + coll.getLatMax()) / 2, (coll.getLonMin() + coll.getLonMax()) / 2);

            res.add(bucket);
        }

        return new InternalGeoHashClustering(name, pipelineAggregators(), metaData(),res, distance, zoom);
    }

    @Override
    public InternalGeoHashClustering buildEmptyAggregation() {
        return new InternalGeoHashClustering(name, pipelineAggregators(), metaData(),
                Collections.<InternalGeoHashClustering.Bucket>emptyList(), distance, zoom);
    }

    @Override
    protected LeafBucketCollector getLeafCollector(final LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        final MultiGeoPointValues geoValues = valuesSource.geoPointValues(ctx);

        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(final int doc, final long bucket) throws IOException {
                assert bucket == 0;

                geoValues.setDocument(doc);
                final int valuesCount = geoValues.count();

                for (int i = 0; i < valuesCount; i++) {
                    final GeoPoint geoPoint = geoValues.valueAt(i);
                    final double meterByPixel = GeoClusterUtils.getMeterByPixel(zoom, geoPoint.getLat());
                    int pointPrecision = GeoUtils.geoHashLevelsForPrecision(distance * meterByPixel);

                    pointPrecision = pointPrecision > 1 ? pointPrecision - 1 : pointPrecision;
                    if (pointPrecision > 12) {
                        pointPrecision = 12;
                    }

                    final long clusterHashLong = GeoHashUtils.longEncode(geoPoint.getLat(), geoPoint.getLon(), pointPrecision);
                    final ClusterCollector collec;
                    long bucketOrdinal2 = bucketOrds.add(clusterHashLong);
                    if (bucketOrdinal2 < 0) { // already seen
                        bucketOrdinal2 = -1 - bucketOrdinal2;
                        collec = clusterCollectors.get(clusterHashLong);
                        collec.addPoint(geoPoint);
                        collectExistingBucket(sub, doc, bucketOrdinal2);
                    } else {
                        collec = new ClusterCollector(geoPoint);
                        collectBucket(sub, doc, bucketOrdinal2);
                    }

                    clusterCollectors.put(clusterHashLong, collec);
                }
            }
        };
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
        Releasables.close(clusterCollectors);
    }
}
