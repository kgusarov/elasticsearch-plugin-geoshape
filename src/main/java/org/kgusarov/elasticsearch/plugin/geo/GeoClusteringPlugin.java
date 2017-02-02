package org.kgusarov.elasticsearch.plugin.geo;

import org.kgusarov.elasticsearch.search.aggregations.bucket.geohashclustering.GeoHashClusteringParser;
import org.kgusarov.elasticsearch.search.aggregations.bucket.geohashclustering.InternalGeoHashClustering;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchModule;

public class GeoClusteringPlugin extends Plugin {
    @Override
    public String name() {
        return "Geo Clustering Plugin";
    }

    @Override
    public String description() {
        return "Geo clustering plugin for Elasticsearch";
    }

    public void onModule(final SearchModule searchModule) {
        searchModule.registerAggregatorParser(GeoHashClusteringParser.class);
        InternalGeoHashClustering.registerStreams();
    }
}
