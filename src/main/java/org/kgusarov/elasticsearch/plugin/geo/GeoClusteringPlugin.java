package org.kgusarov.elasticsearch.plugin.geo;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchModule;

public class GeoClusteringPlugin extends Plugin {
    @Override
    public String name() {
        return "geo-clustering-plugin";
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
