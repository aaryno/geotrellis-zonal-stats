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
import geotrellis.raster.rasterize._
import geotrellis.raster.rasterize.polygon._

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
        .setAppName("Spark Tiler")
        .setMaster("local[*]")
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
      TileLayerMetadata.fromRdd(inputRdd, FloatingLayoutScheme(1024))

    // Use the Tiler to cut our tiles into tiles that are index to a floating layout scheme.
    // We'll repartition it so that there are more partitions to work with, since spark
    // likes to work with more, smaller partitions (to a point) over few and large partitions.
    val tiledRDD: RDD[(SpatialKey, Tile)] =
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout,
        Tiler.Options(resampleMethod=Bilinear, partitioner=new HashPartitioner(1000)))

     val layerRdd: TileLayerRDD[SpatialKey] =
      ContextRDD(tiledRDD, rasterMetaData)

    val filename = "/tmp/all_within_tile_0_0.geojson"
    val fc = Source.fromFile(filename).getLines.mkString.stripMargin

    case class IsoData(ISO: String, ID_1: Int, ID_2: Int)
    implicit val IsoFormat = jsonFormat3(IsoData)

    // this only covers polygons-- need to add multipolygons at some point
    val polygons: Map[String, PolygonFeature[IsoData]] = fc.parseGeoJson[JsonFeatureCollectionMap]
                                                        .getAllPolygonFeatures[IsoData]

      // Sequence op - combine one value with our aggregate value
      val seqOp: (Map[String, Double], Raster[Tile]) => Map[String, Double] =
        { (acc, raster) =>
          val extent = raster.extent
          val env = extent.toPolygon

          polygons
            .filter { tup => env.intersects(tup._2) }
            .map { case (name, polygon) =>
              var v = 0.0
              var cellArea = 10

              if (polygon.contains(env)) {
                // polygon contains this whole tile, so
                // sum up the value of tile
                raster.tile.foreachDouble { z =>
                  if(isData(z)) { v += z * cellArea }
                }
              } else {
                // Polygon only partially intersects the tile,
                // use fractional rasterization.
                val tile = raster.tile
                val intersections =
                  polygon.intersection(env) match {
                    case PolygonResult(intersectionPoly) => Seq(intersectionPoly)
                    case MultiPolygonResult(mp) => mp.polygons.toSeq
                    case _ => Seq()
                  }

                intersections
                  .map { p =>
                    FractionalRasterizer.foreachCellByPolygon(p, raster.rasterExtent)(
                      new FractionCallback {
                        def callback(col: Int, row: Int, fraction: Double): Unit = {
                          v += tile.getDouble(col, row) * cellArea * fraction
                        }
                      }
                    )
                  }
              }

              (name, v)
            }
            .toMap
        }

      // Combine op - combine two aggregate sums
      val combOp: (Map[String, Double], Map[String, Double]) => Map[String, Double] =
        { (acc1, acc2) =>
          (acc1.keySet ++ acc2.keySet)
            .map { k => k -> (acc1.get(k).toList ::: acc2.get(k).toList) }
            .map { case (k, vs) => (k, vs.sum) }
            .toMap
        }

      val volume =
        layerRdd
          .asRasters
          .map(_._2)
          .aggregate(Map[String, Double]())(seqOp, combOp)


  }
}
