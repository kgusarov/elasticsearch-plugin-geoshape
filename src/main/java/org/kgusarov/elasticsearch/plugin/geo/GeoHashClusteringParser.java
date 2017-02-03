package org.kgusarov.elasticsearch.plugin.geo;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.*;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked", "NestedAssignment", "NestedMethodCall"})
public class GeoHashClusteringParser implements Aggregator.Parser {

    @Override
    public String type() {
        return InternalGeoHashClustering.TYPE.name();
    }

    public static final int DEFAULT_ZOOM = 10;
    public static final int DEFAULT_DISTANCE = 100;

    @Override
    public AggregatorFactory parse(final String aggregationName, final XContentParser parser, final SearchContext context) throws IOException {

        final ValuesSourceParser vsParser = ValuesSourceParser.geoPoint(aggregationName, InternalGeoHashClustering.TYPE, context).build();

        int zoom = DEFAULT_ZOOM;
        int distance = DEFAULT_DISTANCE;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (vsParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("zoom".equals(currentFieldName)) {
                    zoom = parser.intValue();
                } else if ("distance".equals(currentFieldName)) {
                    distance = parser.intValue();
                }
            }
        }

        return new GeoGridFactory(aggregationName, vsParser.config(), zoom, distance);
    }

    @SuppressWarnings("SerializableStoresNonSerializable")
    private static final class GeoGridFactory extends ValuesSourceAggregatorFactory<ValuesSource.GeoPoint> {
        private final int zoom;
        private final int distance;

        private GeoGridFactory(final String name, final ValuesSourceConfig<ValuesSource.GeoPoint> config,
                               final int zoom, final int distance) {
            super(name, InternalGeoHashClustering.TYPE.name(), config);
            this.zoom = zoom;
            this.distance = distance;
        }

        @Override
        protected Aggregator createUnmapped(final AggregationContext aggregationContext, final Aggregator parent,
                                            final List<PipelineAggregator> pipelineAggregators,
                                            final Map<String, Object> metaData) throws IOException {

            final InternalAggregation aggregation = new InternalGeoHashClustering(name, pipelineAggregators, metaData,
                    Collections.<InternalGeoHashClustering.Bucket>emptyList(), distance, zoom);

            return new NonCollectingAggregator(name, aggregationContext, parent, pipelineAggregators, metaData) {
                @Override
                public InternalAggregation buildEmptyAggregation() {
                    return aggregation;
                }
            };
        }

        @Override
        protected Aggregator doCreateInternal(final ValuesSource.GeoPoint valuesSource,
                                              final AggregationContext aggregationContext,
                                              final Aggregator parent,
                                              final boolean collectsFromSingleBucket,
                                              final List<PipelineAggregator> pipelineAggregators,
                                              final Map<String, Object> metaData) throws IOException {

            return new GeoHashClusteringAggregator(name, factories, valuesSource,
                    aggregationContext, parent, zoom, distance, pipelineAggregators, metaData);
        }
    }
}