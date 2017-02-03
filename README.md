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
  "size": 0,
  "query": {
    "constant_score": {
      "filter": {
        "geo_bbox": {
          "location": {
            "top_left": [
              23.800994082106058,
              57.06691854468494
            ],
            "bottom_right": [
              24.581023378981058,
              56.80468658971446
            ]
          }
        }
      }
    }
  },
  "aggregations": {
    "places": {
      "geohash_clustering": {
        "field": "location",
        "zoom": 14
      }
    }
  }
}
```

```json
{
  "took": 3,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "failed": 0
  },
  "hits": {
    "total": 7,
    "max_score": 0,
    "hits": [

    ]
  },
  "aggregations": {
    "places": {
      "buckets": [
        {
          "key": "ud15ux",
          "doc_count": 2,
          "geohash_grids": [
            [
              "ud15ux",
              "ud15uw"
            ]
          ],
          "cluster_center": {
            "type": "point",
            "coordinates": [
              24.1101654432714,
              56.94732195697725
            ]
          },
          "cluster_bounds": {
            "top_left": {
              "type": "point",
              "coordinates": [
                24.10761198028922,
                56.94801799021661
              ]
            },
            "bottom_right": {
              "type": "point",
              "coordinates": [
                24.112718906253576,
                56.946625923737884
              ]
            }
          }
        },
        {
          "key": "ud1hj1",
          "doc_count": 1,
          "geohash_grids": [
            [
              "ud1hj1"
            ]
          ],
          "cluster_center": {
            "type": "point",
            "coordinates": [
              24.1331759467721,
              56.95931998081505
            ]
          },
          "cluster_bounds": {
            "top_left": {
              "type": "point",
              "coordinates": [
                24.1331759467721,
                56.95931998081505
              ]
            },
            "bottom_right": {
              "type": "point",
              "coordinates": [
                24.1331759467721,
                56.95931998081505
              ]
            }
          }
        },
        {
          "key": "ud1hhb",
          "doc_count": 1,
          "geohash_grids": [
            [
              "ud1hhb"
            ]
          ],
          "cluster_center": {
            "type": "point",
            "coordinates": [
              24.121418986469507,
              56.95506398566067
            ]
          },
          "cluster_bounds": {
            "top_left": {
              "type": "point",
              "coordinates": [
                24.121418986469507,
                56.95506398566067
              ]
            },
            "bottom_right": {
              "type": "point",
              "coordinates": [
                24.121418986469507,
                56.95506398566067
              ]
            }
          }
        },
        {
          "key": "ud0unt",
          "doc_count": 1,
          "geohash_grids": [
            [
              "ud0unt"
            ]
          ],
          "cluster_center": {
            "type": "point",
            "coordinates": [
              23.8493618555367,
              56.983388951048255
            ]
          },
          "cluster_bounds": {
            "top_left": {
              "type": "point",
              "coordinates": [
                23.8493618555367,
                56.983388951048255
              ]
            },
            "bottom_right": {
              "type": "point",
              "coordinates": [
                23.8493618555367,
                56.983388951048255
              ]
            }
          }
        },
        {
          "key": "ud15fu",
          "doc_count": 2,
          "geohash_grids": [
            [
              "ud15fu"
            ]
          ],
          "cluster_center": {
            "type": "point",
            "coordinates": [
              24.03742292895913,
              56.93228993564844
            ]
          },
          "cluster_bounds": {
            "top_left": {
              "type": "point",
              "coordinates": [
                24.03742292895913,
                56.93228993564844
              ]
            },
            "bottom_right": {
              "type": "point",
              "coordinates": [
                24.03742292895913,
                56.93228993564844
              ]
            }
          }
        }
      ]
    }
  }
}
```
