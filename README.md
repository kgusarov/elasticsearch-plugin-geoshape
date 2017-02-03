# Elasticsearch geohash clustering plugin

This plugin adds a `geohash_clustering` aggregation.

Installation
------------

```
./elasticsearch/bin/plugin install https://github.com/kgusarov/elasticsearch-plugin-geoshape/releases/download/v2.2.0.1/v2.2.0.1.zip
```

| Elasticsearch Version  | Plugin Version      | GitHub Tag |
|------------------------|---------------------| -----------|
| 2.2.0                  | 2.2.0.1             | v2.2.0.1   |

### Geohash clustering aggregation

This aggregations computes a geohash precision from a `zoom` and a `distance` (in pixel).
It groups points (from `field` parameter) into buckets that represent geohash cells and computes each bucket's center.
Then it merges these cells if the distance between two clusters' centers is lower than the `distance` parameter.

```json
{
  "aggregations": {
    "<aggregation_name>": {
      "geohash_clustering": {
        "field": "<field_name>",
        "zoom": "<zoom>"
      }
    }
  }
}
```
Input parameters :
 - `field` must be of type geo_point.
 - `zoom` is a mandatory integer parameter between 0 and 20. It represents the zoom level used in the request to aggregate geo points.

The plugin aggregates these points in geohash with a "good" precision depending on the zoom provided. Then it merges clusters based on distance (in pixels).
Default distance is set to 100, but it can be set to another integer in the request.

For example :

```json
{
    "aggregations" : {
        "my_cluster_aggregation" : {
            "geohash_clustering": {
                "field": "geo_point",
                "zoom": 0,
                "distance": 50
            }
        }
    }
}
```

```json
{
    "aggregations": {
         "my_cluster_aggregation": {
            "buckets": [
               {
                  "key": "u0",
                  "doc_count": 90293,
                  "geohash_grids": [
                     [
                        "u0"
                     ]
                  ],
                  "cluster_center": {
                     "type": "point",
                     "coordinates": [
                        2.32920361762,
                        48.8449899502
                     ]
                  }
               }
            ]
         }
    }

}
```
