package org.kgusarov.elasticsearch.plugin.geo;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.google.common.collect.Lists;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoBoundingBoxQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.collection.IsIterableWithSize.iterableWithSize;

@Repeat(iterations = 100)
@SuppressWarnings("unchecked")
@ESIntegTestCase.ClusterScope(numDataNodes = 4)
public class GeoClusteringPluginTest extends ESIntegTestCase {
    @Test
    public void testGeoClustering() throws Exception {
        final String mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("type1")
                        .startObject("properties")
                            .startObject("location")
                                .field("type", "geo_point")
                                .field("index", "analyzed")
                                .field("geohash", "true")
                                .field("geohash_prefix", "true")
                                .field("geohash_precision", "12")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .string();

        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();

        index("test", "type1", "lido1", '{' +
                "   \"location\": {" +
                "       \"lat\": 56.955064," +
                "       \"lon\": 24.121419" +
                "   }" +
                '}');

        index("test", "type1", "lido2", '{' +
                "   \"location\": {" +
                "       \"lat\": 56.93229," +
                "       \"lon\": 24.037423" +
                "   }" +
                '}');

        index("test", "type1", "gan-bei1", '{' +
                "   \"location\": {" +
                "       \"lat\": 56.948018," +
                "       \"lon\": 24.112719" +
                "   }" +
                '}');

        index("test", "type1", "dcoffee1", '{' +
                "   \"location\": {" +
                "       \"lat\": 56.93229," +
                "       \"lon\": 24.037423" +
                "   }" +
                '}');

        index("test", "type1", "t73", '{' +
                "   \"location\": {" +
                "       \"lat\": 56.95932," +
                "       \"lon\": 24.133176" +
                "   }" +
                '}');

        index("test", "type1", "elsanto", '{' +
                "   \"location\": {" +
                "       \"lat\": 56.946626," +
                "       \"lon\": 24.107612" +
                "   }" +
                '}');

        index("test", "type1", "halipapa", '{' +
                "   \"location\": {" +
                "       \"lat\": 56.983389," +
                "       \"lon\": 23.849362" +
                "   }" +
                '}');

        refresh();
        final SearchResponse all = client().prepareSearch("test")
                .setTypes("type1")
                .get();

        assertSearchHits(all, "lido1", "lido2", "gan-bei1", "dcoffee1", "t73", "elsanto", "halipapa");

        testClustering(100, 10, 2, Lists.newArrayList("ud1h", "ud0u"), Lists.newArrayList(6L, 1L), null);
        testClustering(100, 0, 1, Lists.newArrayList("u"), Lists.newArrayList(7L), null);
        testClustering(100, 20, 6,
                Lists.newArrayList("ud1hhbke", "ud15uwfd", "ud1hj1jh", "ud15uxn6", "ud0untw8", "ud15fupw"),
                Lists.newArrayList(1L, 1L, 1L, 1L, 1L, 2L), null);
        testClustering(100, 14, 5,
                Lists.newArrayList("ud15ux", "ud1hj1", "ud1hhb", "ud0unt", "ud15fu"),
                Lists.newArrayList(2L, 2L, 1L, 1L, 1L), null);

        testClustering(500, 14, 3,
                Lists.newArrayList("ud1hj", "ud0un", "ud15f"),
                Lists.newArrayList(4L, 2L, 1L), null);

        testClustering(100, 14, 5,
                Lists.newArrayList("ud15ux", "ud1hj1", "ud1hhb", "ud0unt", "ud15fu"),
                Lists.newArrayList(2L, 2L, 1L, 1L, 1L),
                srb -> {
                    final GeoBoundingBoxQueryBuilder filter = geoBoundingBoxQuery("location")
                            .topLeft(57.06691854468494, 23.800994082106058)
                            .bottomRight(56.80468658971446, 24.581023378981058);

                    final ConstantScoreQueryBuilder query = constantScoreQuery(filter);
                    srb.setQuery(query);
                }
        );

        testClustering(100, 14, 0,
                Lists.newArrayList(),
                Lists.newArrayList(),
                srb -> {
                    final GeoBoundingBoxQueryBuilder filter = geoBoundingBoxQuery("location")
                            .topLeft(67.06691854468494, 23.800994082106058)
                            .bottomRight(66.80468658971446, 24.581023378981058);

                    final ConstantScoreQueryBuilder query = constantScoreQuery(filter);
                    srb.setQuery(query);
                }
        );

        testClustering(0, 14, 1,
                Lists.newArrayList("ud15fupw8s1d"),
                Lists.newArrayList(2L),
                srb -> {
                    final GeoBoundingBoxQueryBuilder filter = geoBoundingBoxQuery("location")
                            .topLeft(56.93231, 24.037422)
                            .bottomRight(56.93227, 24.037425);

                    final ConstantScoreQueryBuilder query = constantScoreQuery(filter);
                    srb.setQuery(query);
                }
        );

        final InternalGeoHashClustering clustering = testClustering(100, 14, 5,
                Lists.newArrayList("ud15ux", "ud1hj1", "ud1hhb", "ud0unt", "ud15fu"),
                Lists.newArrayList(2L, 2L, 1L, 1L, 1L),
                srb -> {
                    final GeoBoundingBoxQueryBuilder filter = geoBoundingBoxQuery("location")
                            .topLeft(57.06691854468494, 23.800994082106058)
                            .bottomRight(56.80468658971446, 24.581023378981058);

                    final ConstantScoreQueryBuilder query = constantScoreQuery(filter);
                    srb.setQuery(query);
                }
        );

        GeoHashClustering.Bucket bucket = clustering.getBucketByKey("ud15ux");
        assertEquals(
                new ClusterBounds(56.946625923737884, 56.94801799021661, 24.10761198028922, 24.112718906253576),
                bucket.getClusterBounds()
        );

        bucket = clustering.getBucketByKey("ud1hj1");
        assertEquals(
                new ClusterBounds(56.95931998081505, 56.95931998081505, 24.1331759467721, 24.1331759467721),
                bucket.getClusterBounds()
        );

        bucket = clustering.getBucketByKey("ud1hhb");
        assertEquals(
                new ClusterBounds(56.95506398566067, 56.95506398566067, 24.121418986469507, 24.121418986469507),
                bucket.getClusterBounds()
        );

        bucket = clustering.getBucketByKey("ud0unt");
        assertEquals(
                new ClusterBounds(56.983388951048255, 56.983388951048255, 23.8493618555367, 23.8493618555367),
                bucket.getClusterBounds()
        );

        bucket = clustering.getBucketByKey("ud15fu");
        assertEquals(
                new ClusterBounds(56.93228993564844, 56.93228993564844, 24.03742292895913, 24.03742292895913),
                bucket.getClusterBounds()
        );
    }

    private InternalGeoHashClustering testClustering(final int distance, final int zoom, final int expectedBucketCount,
                                                     final Collection<String> expectedBucketGeohashes,
                                                     final Collection<Long> expectedBucketsSizes,
                                                     final Consumer<SearchRequestBuilder> additionalParams) {

        final GeoHashClusteringBuilder clustering = new GeoHashClusteringBuilder("places")
                .field("location")
                .distance(distance)
                .zoom(zoom);

        final SearchRequestBuilder srb = client().prepareSearch("test")
                .setTypes("type1")
                .setSize(0)
                .addAggregation(clustering);

        if (additionalParams != null) {
            additionalParams.accept(srb);
        }

        final SearchResponse clusters = srb.get();

        final Aggregation places = clusters.getAggregations().get("places");
        assertThat(places, IsInstanceOf.instanceOf(InternalGeoHashClustering.class));

        final InternalGeoHashClustering geoHashClustering = (InternalGeoHashClustering) places;
        final List<GeoHashClustering.Bucket> buckets = geoHashClustering.getBuckets();

        final String[] s = expectedBucketGeohashes.stream().toArray(String[]::new);
        assertThat(buckets, iterableWithSize(expectedBucketCount));
        assertThat(buckets.stream()
                        .map(GeoHashClustering.Bucket::getKeyAsString)
                        .collect(Collectors.toList()),
                containsInAnyOrder(s));

        final Long[] l = expectedBucketsSizes.stream().toArray(Long[]::new);
        assertThat(buckets.stream()
                        .map(GeoHashClustering.Bucket::getDocCount)
                        .collect(Collectors.toList()),
                containsInAnyOrder(l));

        return geoHashClustering;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Lists.newArrayList(GeoClusteringPlugin.class);
    }
}
