/*
 * Author: Neel Gandhi
 * 
 * Pre-Conditions: Hadoop is running using ./start-dfs, Mentioned files paths are valid in HDFS
 * Instructions to run:
 * 1) mvn clean package   // Exports a jar file called geospatial-spark-0.0.1-SNAPSHOT.jar in the target directory
 * 2) ./dev/spark/bin/spark-submit --class "geometricfarthestpoints.FarthestPair" --master local[2] geospatial-apachespark/target/geospatial-spark-0.0.1-SNAPSHOT.jar
 * 
 * Date Created: Mar 23, 2015
 */

package geometricfarthestpoints;

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

public class FarthestPair {
	/**
	 * A dummy tester for Farthest Pair
	 * 
	 * @param args
	 */

	public static void main(String[] args) {
		// String inp1 =
		// "/home/hduser/dev/geospatial-apachespark/data/range_inp1";
		// String inp2 =
		// "/home/hduser/dev/geospatial-apachespark/data/range_inp1";

		String inp1 = "closest_inp2"; // in my HDFS
		String out = "farthest_out_" + Utils.getEpochTick();

		farthestPair(inp1, out);

	}

	public static boolean farthestPair(String rectanglesFilePath, String ouputFilePath) {
		SparkConf conf = new SparkConf().setAppName("Farthest Pair Module");
		JavaSparkContext sc = new JavaSparkContext(conf);
		boolean result = farthestPairHelper(rectanglesFilePath, ouputFilePath, sc);

		sc.close();
		return result;
	}

	@SuppressWarnings("serial")
	private static boolean farthestPairHelper(String pointsFilePath, String ouputFilePath, JavaSparkContext sc) {
		try {
			JavaRDD<String> pointStrings = sc.textFile(pointsFilePath);
			if (Settings.D)
				Utils.Log("Fetched Points");

			// Typecast Points
			JavaRDD<Point> pointsRDD = pointStrings.map(new Function<String, Point>() {
				public Point call(String s) {
					Float[] nums = Utils.splitStringToFloat(s, ",");
					return new Point(nums[0], nums[1]);
				}
			}).cache();
			if (Settings.D)
				Utils.Log("Created Point Objects");
			if (Settings.D)
				Utils.Log("First Point: " + pointsRDD.first());

			// TODO: Avoid collection
			final List<Point> points = pointsRDD.collect();

			JavaPairRDD<String, PairPoints> pairs = pointsRDD.mapToPair(new PairFunction<Point, String, PairPoints>() {
				public Tuple2<String, PairPoints> call(Point s) {
					// Compute farthest point to this point
					float maxDist = 0;
					float d;
					Point farthest = s;
					// TODO: Use Spark's foreach or foreachAsync
					for (Point pxy : points) {
						d = s.getDistance(pxy);
						// a point is farthest to itself i.e. d = 0, hence we ignore such comparisons
						// Hence we need to assume that all points in the input are unique
						if (d > 0 && maxDist < d) {
							maxDist = d;
							farthest = pxy;
						}
					}

					return new Tuple2<String, PairPoints>("p", new PairPoints(s, farthest, maxDist));
				}
			});
			if (Settings.D)
				Utils.Log("Farthest First" + pairs.first());

			// TODO: Try to use min function, using PairPointsMinComparator.java
			// pairs.min(new PairPointsMinComparator().);

			JavaPairRDD<String, PairPoints> maxoffarthest = pairs
					.reduceByKey(new Function2<PairPoints, PairPoints, PairPoints>() {
						public PairPoints call(PairPoints pts1, PairPoints pts2) throws Exception {
							// If points in pts1 are closer than points in pts2, then pts1 is less than pts2
							if (pts1.getDistance() > pts2.getDistance())
								return pts1;
							return pts2;
						}
					});
			Tuple2<String, PairPoints> ans = maxoffarthest.first();
			if (Settings.D)
				Utils.Log("Farthest Pair" + ans);

			maxoffarthest.map(new Function<Tuple2<String, PairPoints>, String>() {
				public String call(Tuple2<String, PairPoints> t) throws Exception {
					Point p1 = t._2().getP1();
					Point p2 = t._2().getP2();
					return p1.asSimpleString() + "\r\n" + p2.asSimpleString();
				}
			}).saveAsTextFile(ouputFilePath);

			Utils.Log("Done!");

			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
}
