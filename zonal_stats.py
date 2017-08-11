import json
import os
import csv
from functools import partial

# set SPARK_HOME
os.environ["SPARK_HOME"] = r"/usr/lib/spark"

from geopyspark.geopycontext import GeoPyContext
from geopyspark.geotrellis.constants import SPATIAL
from geopyspark.geotrellis.geotiff_rdd import get

from shapely.geometry import shape


# Create the GeoPyContext
geopysc = GeoPyContext(appName="example", master="yarn")

# read in a raster saved locally-- a 5x5 degree tile
# of float biomass data in the central Amazon
raster_rdd = get(geopysc=geopysc, rdd_type=SPATIAL,
uri = '/tmp/tile__0_0.tif',
options={'numPartitions': 100}
)

tiled_rdd = raster_rdd.to_tiled_layer()

# load the admin2 level geometries that we'll use to summarize our data
with open('all_within_tile_0_0.geojson') as f:
    txt = json.load(f)

with open('out.csv', 'w') as thefile:
    csvwriter = csv.writer(thefile)
    for f in txt['features']:
        geom = shape(f['geometry'])

        sum_val = tiled_rdd.polygonal_sum(geometry=geom, data_type=float)
        props = f['properties']
        out_row = [props['ISO'], props['ID_1'], props['ID_2'], sum_val]
        print(out_row)
        csvwriter.writerow(out_row)

