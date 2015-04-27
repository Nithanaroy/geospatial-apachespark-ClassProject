/*
 * Author: Nitin Pasumarthy
 * 
 * Pre-Conditions: Hadoop is running using ./start-dfs, Mentioned files paths are valid in HDFS
 * Instructions to run:
 * 1) mvn clean package   // Exports a jar file called geospatial-spark-0.0.1-SNAPSHOT.jar in the target directory
 * 2) ./dev/spark/bin/spark-submit --class "geometricclosestpoints.ClosestPair" --master local[2] geospatial-apachespark/target/geospatial-spark-0.0.1-SNAPSHOT.jar
 * 
 * Date Created: Mar 13, 2015
 */

package geometricclosestpoints;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import common.PairPoints;
import common.Point;
import common.Rectangle;
import common.Settings;
import common.Utils;

public class ClosestPair {
	/**
	 * A dummy tester for Closest Pair
	 * 
	 * @param args
	 */

	// Some random number which is big enough to ensure the points are at least closer than this value
	// We will initialize the minimum distance to this and start comparing with all points
	// Hence the assumption is that 2 points in the input set are at least closer than this value
	private static float minDistanceInitalizer = 10000000;

	public static void main(String[] args) {
		// String inp1 =
		// "/home/hduser/dev/geospatial-apachespark/data/range_inp1";
		// String inp2 =
		// "/home/hduser/dev/geospatial-apachespark/data/range_inp1";

		// String inp1 = "closest_inp1"; // in my HDFS

		String inp1 = args[0]; // in my HDFS
		int partitions = -1;
		try {
			partitions = Integer.parseInt(args[1]);
		} catch (Exception e) {
		}
		String out = "closest_out_" + Utils.getEpochTick();

		closestPair(inp1, out, partitions);

	}

	public static boolean closestPair(String rectanglesFilePath, String ouputFilePath, int partitions) {
		SparkConf conf = new SparkConf().setAppName("Closest Pair Module");
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kyro.registrationRequired", "true");
		JavaSparkContext sc = new JavaSparkContext(conf);
		boolean result = closestPairHelper(rectanglesFilePath, ouputFilePath, partitions, sc);

		sc.close();
		return result;
	}

	@SuppressWarnings("serial")
	private static boolean closestPairHelper(String pointsFilePath, String outputFilePath, int partitions,
			JavaSparkContext sc) {
		try {
			JavaRDD<String> pointStrings = sc.textFile(pointsFilePath);
			if (Settings.D)
				Utils.Log("Fetched Points");

			// Typecast Points
			// schema 1: geom,gid,x1,y1,x2,y2,statefp,countyfp,ansicode,hydroid,fullname,mtfcc,aland,awater,intptlat,intptlon + 2 unknowns
			// schema 2: geom,gid,x1,y1,x2,y2,statefp,ansicode,areaid,fullname,mtfcc,aland,awater,intptlat,intptlon
			// schema 3: x1,y1
			JavaRDD<Point> _pointsRDD = pointStrings.map(new Function<String, Point>() {
				public Point call(String s) {
					Point p = null;
					Float[] nums = Utils.splitStringToFloat(s, ",");
					switch (nums.length) {
					case 18:
						// schema 1
						if (Settings.D)
							Utils.Log("Detected Schema 1");
						p = new Point(nums[2], nums[3]);
						break;

					case 15:
						// schema 2
						if (Settings.D)
							Utils.Log("Detected Schema 2");
						p = new Point(nums[2], nums[3]);
						break;

					case 2:
						// schema 3
						if (Settings.D)
							Utils.Log("Detected Schema 3");
						p = new Point(nums[0], nums[1]);
						break;

					default:
						// unknown schema
						// throw new IllegalArgumentException("Unknown Schema");
						// Ignore lines when schema is unknown
					}
					return p;
				}
			}).filter(new Function<Point, Boolean>() {
				public Boolean call(Point p) {
					return p != null;
				}
			}).cache();
			if (Settings.D)
				Utils.Log("Created Point Objects");
			// if (Settings.D)
			// Utils.Log("First Point: " + pointsRDD.first());

			// TODO: Avoid collection
			final Broadcast<List<Point>> _points = sc.broadcast(_pointsRDD.collect());
//			final List<Point> points = _pointsRDD.collect();
			
			JavaRDD<Point> pointsRDD = null;
			if (partitions > 0) {
				pointsRDD = _pointsRDD.repartition(partitions);
			} else {
				pointsRDD = _pointsRDD;
			}

			// For every point for its closest point and save the Tuple <p1, p2, d>
			// Here p1's closest point is p2 and is at a distance, d
			JavaPairRDD<String, PairPoints> pairs = pointsRDD.mapToPair(new PairFunction<Point, String, PairPoints>() {
				public Tuple2<String, PairPoints> call(Point s) {
					// Compute closest point to this point
					float minDist = minDistanceInitalizer, d;
					Point closest = s;
					// TODO: Use Spark's foreach or foreachAsync
					List<Point> points = _points.getValue();
					for (Point pxy : points) {
						d = s.getDistance(pxy);
						// a point is closest to itself i.e. d = 0, hence we ignore such comparisons
						// Hence we need to assume that all points in the input are unique
						if (d > 0 && minDist > d) {
							minDist = d;
							closest = pxy;
						}
					}

					return new Tuple2<String, PairPoints>("p", new PairPoints(s, closest, minDist));
				}
			});
			// if (Settings.D)
			// Utils.Log("Closest First" + pairs.first());

			// TODO: Try to use min function, using PairPointsMinComparator.java
			// pairs.min(new PairPointsMinComparator().);

			JavaPairRDD<String, PairPoints> minofclosest = pairs
					.reduceByKey(new Function2<PairPoints, PairPoints, PairPoints>() {
						public PairPoints call(PairPoints pts1, PairPoints pts2) throws Exception {
							// If points in pts1 are closer than points in pts2, then pts1 is less than pts2
							if (pts1.getDistance() < pts2.getDistance())
								return pts1;
							return pts2;
						}
					});
			
			minofclosest.saveAsTextFile(outputFilePath);
			
			// Tuple2<String, PairPoints> ans = minofclosest.first();
			// if (Settings.D)
			// Utils.Log("Closest Pair" + ans);
			//
			// minofclosest.map(new Function<Tuple2<String, PairPoints>, String>() {
			// public String call(Tuple2<String, PairPoints> t) throws Exception {
			// Point p1 = t._2().getP1();
			// Point p2 = t._2().getP2();
			// return p1.asSimpleString() + "\r\n" + p2.asSimpleString();
			// }
			// }).saveAsTextFile(ouputFilePath);

			Utils.Log("Done!");

			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
}
