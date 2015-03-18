/*
 * Author: Nitin Pasumarthy
 * 
 * Pre-Conditions: Hadoop is running using ./start-dfs, Mentioned files paths are valid in HDFS
 * Instructions to run:
 * 1) mvn clean package   // Exports a jar file called geospatial-spark-0.0.1-SNAPSHOT.jar in the target directory
 * 2) ./dev/spark/bin/spark-submit --class "geometricclosestpoints.ClosestPair" --master local[4] geospatial-apachespark/target/geospatial-spark-0.0.1-SNAPSHOT.jar
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

import scala.Tuple2;

import common.PairPoints;
import common.Point;
import common.Settings;
import common.Utils;

public class ClosestPair {
	/**
	 * A dummy tester for Closest Pair
	 * 
	 * @param args
	 */

	// Some randome number which is big enough to ensure the points are at least closer than this value
	// We will initialize the minimum distance to this and start comparing with all points
	// Hence the assumption is that 2 points in the input set are at least closer than this value
	private static float minDistanceInitalizer = 10000000;

	public static void main(String[] args) {
		// String inp1 =
		// "/home/hduser/dev/geospatial-apachespark/data/range_inp1";
		// String inp2 =
		// "/home/hduser/dev/geospatial-apachespark/data/range_inp1";

		String inp1 = "closest_inp1"; // in my HDFS
		String out = "closest_out_" + Utils.getEpochTick();

		closestPair(inp1, out);

	}

	public static boolean closestPair(String rectanglesFilePath, String ouputFilePath) {
		SparkConf conf = new SparkConf().setAppName("Closest Pair Module");
		JavaSparkContext sc = new JavaSparkContext(conf);
		boolean result = closestPairHelper(rectanglesFilePath, ouputFilePath, sc);

		sc.close();
		return result;
	}

	@SuppressWarnings("serial")
	private static boolean closestPairHelper(String pointsFilePath, String ouputFilePath, JavaSparkContext sc) {
		try {
			JavaRDD<String> p = sc.textFile(pointsFilePath);
			if (Settings.D)
				Utils.Log("Fetched Points");

			// Typecast Points
			JavaRDD<Point> p1 = p.map(new Function<String, Point>() {
				public Point call(String s) {
					Float[] nums = Utils.splitStringToFloat(s, ",");
					return new Point(nums[0], nums[1]);
				}
			}).cache();
			if (Settings.D)
				Utils.Log("Created Point Objects");
			if (Settings.D)
				Utils.Log("First Point: " + p1.first());

			// TODO: Avoid collection
			final List<Point> p2 = p1.collect();

			JavaPairRDD<String, PairPoints> pairs = p1.mapToPair(new PairFunction<Point, String, PairPoints>() {
				public Tuple2<String, PairPoints> call(Point s) {
					// Compute closest point to this point
					float minDist = minDistanceInitalizer, d;
					Point closest = s;
					// TODO: Use Spark's foreach or foreachAsync
					for (Point pxy : p2) {
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
			if (Settings.D)
				Utils.Log("Closest First" + pairs.first());

			// TODO: Try to use min function, using PairPointsMinComparator.java
			// pairs.min(new PairPointsMinComparator().);

			JavaPairRDD<String, PairPoints> counts = pairs
					.reduceByKey(new Function2<PairPoints, PairPoints, PairPoints>() {
						public PairPoints call(PairPoints pts1, PairPoints pts2) throws Exception {
							// If points in pts1 are closer than points in pts2, then pts1 is less than pts2
							if (pts1.getDistance() < pts2.getDistance())
								return pts1;
							return pts2;
						}
					});
			if (Settings.D)
				Utils.Log("Closest of Closest First" + counts.first());

			Utils.Log("Done!");

			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
}
