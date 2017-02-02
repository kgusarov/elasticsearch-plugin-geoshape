package org.kgusarov.elasticsearch.search.aggregations.bucket.geohashclustering;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.io.IOException;

public class GeoHashClusteringBuilder extends AggregationBuilder<GeoHashClusteringBuilder> {
    private String field;
    private int zoom = GeoHashClusteringParser.DEFAULT_ZOOM;
    private int distance = GeoHashClusteringParser.DEFAULT_DISTANCE;

    public GeoHashClusteringBuilder(final String name) {
        super(name, InternalGeoHashClustering.TYPE.name());
    }

    public GeoHashClusteringBuilder field(final String field) {
        this.field = field;
        return this;
    }

    public GeoHashClusteringBuilder zoom(final int zoom) {
        this.zoom = zoom;
        return this;
    }

    public GeoHashClusteringBuilder distance(final int distance) {
        this.distance = distance;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field("field", field);
        }
        if (zoom != GeoHashClusteringParser.DEFAULT_ZOOM) {
            builder.field("zoom", zoom);
        }

        if (distance != GeoHashClusteringParser.DEFAULT_DISTANCE) {
            builder.field("distance", distance);
        }
        return builder.endObject();
    }
}
