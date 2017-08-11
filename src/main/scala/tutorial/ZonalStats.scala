package tutorial

import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.summary.polygonal._

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.spark.tiling._

import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json._

import org.apache.spark._
import org.apache.spark.rdd._

import spray.json._
import spray.json.DefaultJsonProtocol._

import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.reader.GeoTiffReader.GeoTiffInfo
import geotrellis.spark.io.s3._

import scala.io.StdIn
import scala.io.Source
import java.io.File

object ZonalStats {

  def main(args: Array[String]): Unit = {
    // Setup Spark to use Kryo serializer.
    val conf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("Spark Tiler")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    try {
      run(sc)
      // Pause to wait to close the spark context,
      // so that you can check out the UI at http://localhost:4040
      println("Hit enter to exit.")
      StdIn.readLine()
    } finally {
      sc.stop()
    }
  }

  def fullPath(path: String) = new java.io.File(path).getAbsolutePath

  def run(implicit sc: SparkContext) = {
     val inputRdd: RDD[(ProjectedExtent, Tile)] = 
        S3GeoTiffRDD.spatial("gfw2-data", "alerts-tsv/temp/rasters")

    // Use the "TileLayerMetadata.fromRdd" call to find the zoom
    // level that the closest match to the resolution of our source image,
    // and derive information such as the full bounding box and data type.
    val (_, rasterMetaData) =
      TileLayerMetadata.fromRdd(inputRdd, FloatingLayoutScheme(512))

    // Use the Tiler to cut our tiles into tiles that are index to a floating layout scheme.
    // We'll repartition it so that there are more partitions to work with, since spark
    // likes to work with more, smaller partitions (to a point) over few and large partitions.
    val tiledRDD: RDD[(SpatialKey, Tile)] =
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        .repartition(100)

     val layerRdd: TileLayerRDD[SpatialKey] =
      ContextRDD(tiledRDD, rasterMetaData)

    val extent = rasterMetaData.extent

    val filename = "/tmp/all_within_tile_0_0.geojson"
    val fc = Source.fromFile(filename).getLines.mkString.stripMargin
    val features = fc.parseGeoJson[JsonFeatureCollection].getAllPolygons()

    def calc (poly: Polygon, layer: TileLayerRDD[SpatialKey]) : Double = {
          val result = layer.polygonalSumDouble(poly)
          println(result)
          return result

    }

    features.map(f => calc(f, layerRdd))
    

  }
}
