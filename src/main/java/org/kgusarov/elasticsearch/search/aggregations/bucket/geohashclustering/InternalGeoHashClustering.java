package org.kgusarov.elasticsearch.search.aggregations.bucket.geohashclustering;

import com.spatial4j.core.distance.DistanceUtils;
import org.apache.lucene.util.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Represents a grid of cells where each cell's location is determined by a geohash.
 * All geohashes in a grid are of the same precision and held internally as a single long
 * for efficiency's sake.
 */
@SuppressWarnings({"NestedMethodCall", "unchecked"})
public class InternalGeoHashClustering extends InternalMultiBucketAggregation<InternalGeoHashClustering, InternalGeoHashClustering.Bucket>
        implements GeoHashClustering {

    public static final Type TYPE = new Type("geohash_clustering", "ghclustering");

    public static final AggregationStreams.Stream STREAM = in -> {
        final InternalGeoHashClustering buckets1 = new InternalGeoHashClustering();
        buckets1.readFrom(in);
        return buckets1;
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    public static class Bucket extends GeoHashClustering.Bucket {
        long geohashAsLong;
        Set<Long> geohashesList;
        long docCount;
        GeoPoint centroid;
        InternalAggregations aggregations;

        Bucket(final long geohashAsLong, final GeoPoint centroid, final long docCount, final InternalAggregations aggregations) {
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.geohashAsLong = geohashAsLong;
            geohashesList = new HashSet<>();
            this.centroid = centroid;
        }

        @Override
        public GeoPoint getKeyAsGeoPoint() {
            return GeoPoint.fromGeohash(geohashAsLong);
        }

        @Override
        public Number getKeyAsNumber() {
            return geohashAsLong;
        }

        long getGeohashAsLong() {
            return geohashAsLong;
        }

        void setGeohashAsLong(final long geohashAsLong) {
            this.geohashAsLong = geohashAsLong;
        }

        @Override
        public GeoPoint getClusterCenter() {
            return centroid;
        }

        @Override
        public String getKey() {
            return GeoHashUtils.stringEncode(geohashAsLong);
        }

        @Override
        public String getKeyAsString() {
            return getKey();
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        public Bucket reduce(final List<? extends Bucket> buckets, final ReduceContext reduceContext) {
            final List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
            Bucket reduced = null;
            for (final Bucket bucket : buckets) {
                if (reduced == null) {
                    reduced = bucket;
                } else {
                    reduced.docCount += bucket.docCount;
                }
                aggregationsList.add(bucket.aggregations);
            }
            reduced.aggregations = InternalAggregations.reduce(aggregationsList, reduceContext);
            return reduced;
        }

        public void merge(final Bucket bucketToMergeIn, final ReduceContext reduceContext) {

            final long mergedDocCount = bucketToMergeIn.docCount + docCount;
            final double newCentroidLat = (centroid.getLat() * docCount + bucketToMergeIn.centroid.getLat() * bucketToMergeIn.docCount) / mergedDocCount;
            final double newCentroidLon = (centroid.getLon() * docCount + bucketToMergeIn.centroid.getLon() * bucketToMergeIn.docCount) / mergedDocCount;
            bucketToMergeIn.centroid = new GeoPoint(newCentroidLat, newCentroidLon);
            bucketToMergeIn.geohashesList.addAll(geohashesList);
            bucketToMergeIn.docCount = mergedDocCount;

            final List<InternalAggregations> aggregationsList = new ArrayList<>();
            aggregationsList.add(aggregations);
            aggregationsList.add(bucketToMergeIn.aggregations);

            bucketToMergeIn.aggregations = InternalAggregations.reduce(aggregationsList, reduceContext);
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            geohashAsLong = in.readLong();
            final int size = readSize(in);
            final Set<Long> geohashes = new HashSet<>(size);

            for (int i = 0; i < size; i++) {
                final long l = in.readLong();
                geohashes.add(l);
            }

            geohashesList = geohashes;
            docCount = in.readLong();
            centroid = new GeoPoint(in.readDouble(), in.readDouble());
            aggregations = InternalAggregations.readAggregations(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeLong(geohashAsLong);
            writeSize(geohashesList.size(), out);
            for (final Long l : geohashesList) {
                out.writeLong(l);
            }

            out.writeLong(docCount);
            final double lat = centroid.getLat();
            final double lon = centroid.getLon();

            out.writeDouble(lat);
            out.writeDouble(lon);
            aggregations.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            final double lat = centroid.getLat();
            final double lon = centroid.getLon();

            builder.startObject();
            builder.field("geohashAsLong", geohashAsLong);
            final Object[] values = geohashesList.stream().toArray(Long[]::new);
            builder.array("geohashesList", values);
            builder.field("docCount", docCount);
            builder.field("lat", lat);
            builder.field("lon", lon);

            aggregations.toXContent(builder, params);

            builder.endObject();

            return builder;
        }
    }

    private List<Bucket> buckets;
    private Map<String, Bucket> bucketMap;
    private int distance;
    private int zoom;

    InternalGeoHashClustering() {
    } // for serialization

    public InternalGeoHashClustering(final String name, final List<PipelineAggregator> pipelineAggregators,
                                     final Map<String, Object> metaData, final List<Bucket> buckets,
                                     final int distance, final int zoom) {

        super(name, pipelineAggregators, metaData);

        this.buckets = buckets;
        this.distance = distance;
        this.zoom = zoom;
    }

    @Override
    public InternalGeoHashClustering create(final List<Bucket> buckets) {
        return new InternalGeoHashClustering(name, pipelineAggregators(), getMetaData(), buckets, distance, zoom);
    }

    @Override
    public Bucket createBucket(final InternalAggregations aggregations, final Bucket prototype) {
        return new Bucket(prototype.geohashAsLong, prototype.centroid, prototype.docCount, aggregations);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public List<GeoHashClustering.Bucket> getBuckets() {
        final Object o = buckets;
        return (List<GeoHashClustering.Bucket>) o;
    }

    @Override
    public GeoHashClustering.Bucket getBucketByKey(final String geohash) {
        if (bucketMap == null) {
            bucketMap = new HashMap<>(buckets.size());
            for (final Bucket bucket : buckets) {
                bucketMap.put(bucket.getKey(), bucket);
            }
        }
        return bucketMap.get(geohash);
    }

    @Override
    public GeoHashClustering.Bucket getBucketByKey(final Number key) {
        return getBucketByKey(GeoHashUtils.stringEncode(key.longValue()));
    }

    @Override
    public GeoHashClustering.Bucket getBucketByKey(final GeoPoint key) {
        return getBucketByKey(key.geohash());
    }

    @Override
    public InternalGeoHashClustering doReduce(final List<InternalAggregation> aggregations, final ReduceContext reduceContext) {
        LongObjectPagedHashMap<List<Bucket>> buckets = null;

        for (final InternalAggregation aggregation : aggregations) {
            final InternalGeoHashClustering grid = (InternalGeoHashClustering) aggregation;
            if (buckets == null) {
                buckets = new LongObjectPagedHashMap<>(grid.buckets.size(), reduceContext.bigArrays());
            }

            for (final Bucket bucket : grid.buckets) {
                List<Bucket> existingBuckets = buckets.get(bucket.geohashAsLong);
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<>(aggregations.size());
                    buckets.put(bucket.geohashAsLong, existingBuckets);
                }
                existingBuckets.add(bucket);
            }
        }

        final LongObjectPagedHashMap<Bucket> clusterMap = new LongObjectPagedHashMap<>(buckets.size(), reduceContext.bigArrays());
        final List<Long> bucketList = new ArrayList<>();

        for (final LongObjectPagedHashMap.Cursor<List<Bucket>> cursor : buckets) {
            final List<Bucket> sameCellBuckets = cursor.value;
            final Bucket bucket = sameCellBuckets.get(0).reduce(sameCellBuckets, reduceContext);
            clusterMap.put(sameCellBuckets.get(0).geohashAsLong, bucket);
            bucketList.add(sameCellBuckets.get(0).geohashAsLong);
        }

        Collections.sort(bucketList);

        final Iterator<Long> iterBucket = bucketList.iterator();
        loop1:
        while (iterBucket.hasNext()) {
            final Long bucketHash = iterBucket.next();
            final Bucket bucket = clusterMap.get(bucketHash);
            final Collection<? extends CharSequence> neighbors = GeoHashUtils.neighbors(bucket.getKey());

            for (final CharSequence neighbor : neighbors) {
                final String neigh = neighbor.toString();
                final GeoPoint geoPointNeighbor = GeoPoint.fromGeohash(neigh);
                Bucket neighborBucket = clusterMap.get(GeoHashUtils.longEncode(geoPointNeighbor.getLat(),
                        geoPointNeighbor.getLon(), neigh.length()));
                if (neighborBucket == null) {
                    // We test parent neighbor
                    if (neigh.length() > 1) {
                        neighborBucket = clusterMap.get(GeoHashUtils.longEncode(geoPointNeighbor.getLat(),
                                geoPointNeighbor.getLon(), neigh.length() - 1));
                    }
                    if (neighborBucket == null) {
                        continue;
                    }
                }
                if (neighborBucket.geohashesList.contains(bucket.geohashAsLong)) {
                    continue;
                }

                if (shouldCluster(bucket, neighborBucket)) {
                    bucket.merge(neighborBucket, reduceContext);
                    for (final long superClusterHash : bucket.geohashesList) {
                        clusterMap.put(superClusterHash, neighborBucket);
                    }
                    iterBucket.remove();
                    continue loop1;
                }
            }
        }

        final Set<Long> added = new HashSet<>();
        final List<Bucket> res = new ArrayList<>();
        for (final LongObjectPagedHashMap.Cursor<Bucket> cursor : clusterMap) {
            final Bucket buck = cursor.value;
            if (added.contains(buck.geohashAsLong)) {
                continue;
            }
            res.add(buck);
            added.add(buck.geohashAsLong);
        }

        // Add sorting
        return new InternalGeoHashClustering(getName(), pipelineAggregators(), getMetaData(), res, distance, zoom);
    }

    public boolean shouldCluster(final Bucket bucket, final Bucket bucket2) {
        final double lat1 = bucket.getClusterCenter().getLat();
        final double lon1 = bucket.getClusterCenter().getLon();
        final double lat2 = bucket2.getClusterCenter().getLat();
        final double lon2 = bucket2.getClusterCenter().getLon();

        final double curDistance = GeoUtils.EARTH_MEAN_RADIUS * DistanceUtils.distHaversineRAD(
                DistanceUtils.toRadians(lat1), DistanceUtils.toRadians(lon1),
                DistanceUtils.toRadians(lat2), DistanceUtils.toRadians(lon2));

        final double meterByPixel = GeoClusterUtils.getMeterByPixel(zoom, (lat1 + lat2) / 2);

        return distance >= curDistance / meterByPixel;

    }

    @Override
    public void doReadFrom(final StreamInput in) throws IOException {
        distance = in.readInt();
        zoom = in.readInt();
        name = in.readString();
        final int size = in.readVInt();
        final List<Bucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {

            final double centroidLat = in.readDouble();
            final double centroidLon = in.readDouble();
            final int nbGeohash = in.readInt();
            final Set<Long> geohashList = new HashSet<>(nbGeohash);
            for (int j = 0; j < nbGeohash; j++) {
                geohashList.add(in.readLong());
            }
            final Bucket bucket = new Bucket(in.readLong(), new GeoPoint(centroidLat, centroidLon), in.readVLong(),
                    InternalAggregations.readAggregations(in));

            bucket.geohashesList = geohashList;
            buckets.add(bucket);
        }
        this.buckets = buckets;
        bucketMap = null;
    }

    @Override
    public void doWriteTo(final StreamOutput out) throws IOException {
        out.writeInt(distance);
        out.writeInt(zoom);
        out.writeString(name);
        out.writeVInt(buckets.size());
        for (final Bucket bucket : buckets) {
            out.writeDouble(bucket.getClusterCenter().getLat());
            out.writeDouble(bucket.getClusterCenter().getLon());

            out.writeInt(bucket.geohashesList.size());
            for (final long geohash : bucket.geohashesList) {
                out.writeLong(geohash);
            }

            out.writeLong(bucket.geohashAsLong);
            out.writeVLong(bucket.getDocCount());
            ((InternalAggregations) bucket.getAggregations()).writeTo(out);
        }
    }

    @Override
    public XContentBuilder doXContentBody(final XContentBuilder builder, final Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS);
        for (final Bucket bucket : buckets) {
            builder.startObject();
            builder.field(CommonFields.KEY, bucket.getKeyAsString());
            builder.field(CommonFields.DOC_COUNT, bucket.getDocCount());
            final Set<String> geohashGridsString = new HashSet<>(bucket.geohashesList.size());
            geohashGridsString.addAll(bucket.geohashesList.stream()
                    .map(GeoHashUtils::stringEncode)
                    .collect(Collectors.toList()));

            builder.array(new XContentBuilderString("geohash_grids"), geohashGridsString);
            builder.field(new XContentBuilderString("cluster_center"));
            ShapeBuilder.newPoint(bucket.getClusterCenter().getLon(), bucket.getClusterCenter().getLat()).toXContent(builder, params);
            ((InternalAggregations) bucket.getAggregations()).toXContentInternal(builder, params);
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }
}
