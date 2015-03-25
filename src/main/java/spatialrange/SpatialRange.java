/*
 * Author: Nitin Pasumarthy
 * 
 * Pre-Conditions: Hadoop is running using ./start-dfs, Mentioned files paths are valid in HDFS
 * Instructions to run:
 * 1) mvn clean package   // Exports a jar file called geospatial-spark-0.0.1-SNAPSHOT.jar in the target directory
 * 2) ./dev/spark/bin/spark-submit --class "spatialrange.SpatialRange" --master local[4] geospatial-apachespark/target/geospatial-spark-0.0.1-SNAPSHOT.jar
 * 
 * Date Created: Mar 13, 2015
 */

package spatialrange;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import common.Rectangle;
import common.Settings;
import common.Utils;

/**
 * Answer for Question 5 which filters the polygons which completely fall inside a query window provided
 */
public class SpatialRange {

	/**
	 * A dummy tester for Spatial Range
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// String inp1 =
		// "/home/hduser/dev/geospatial-apachespark/data/range_inp1";
		// String inp2 =
		// "/home/hduser/dev/geospatial-apachespark/data/range_inp1";

		String inp1 = "range_inp3"; // in my HDFS
		String inp2 = "range_inp4"; // in my HDFS
		String out = "range_out_" + Utils.getEpochTick();

		spatialRange(inp1, inp2, out);

	}

	/**
	 * The wrapper for Spatial Range Helper. This is the function which user will call.
	 * 
	 * @param rectanglesFilePath
	 *            Input file path having all rectangles
	 * @param queryWindowFilePath
	 *            Input file path in HDFS having the query window
	 * @param ouputFilePath
	 *            Path in HDFS where to save the output
	 * @return true if everything is successful, else false
	 */
	public static boolean spatialRange(String rectanglesFilePath, String queryWindowFilePath, String ouputFilePath) {
		SparkConf conf = new SparkConf().setAppName("Spatial Range Module");
		JavaSparkContext sc = new JavaSparkContext(conf);
		boolean result = spatialRangeHelper(rectanglesFilePath, queryWindowFilePath, ouputFilePath, sc);

		sc.close();
		return result;
	}

	/**
	 * The helper method which performs all computation for Spatial Range query 1) Fetches the rectangles from inp1 in HDFS 2) Fetches the query window(s) from inp2 in HDFS 3) Type casts rectangles and query windows to Rectangle class 4) Filters only those rectangles whose all corners lie within the query window 5) Saves the IDs of those filtered rectangles to HDFS
	 * 
	 * @param rectanglesFilePath
	 *            Input file path having all rectangles
	 * @param queryWindowFilePath
	 *            Input file path in HDFS having the query window
	 * @param ouputFilePath
	 *            Path in HDFS where to save the output
	 * @param sc
	 *            Spark Context
	 * @return true if everything is successful, else false
	 */
	@SuppressWarnings("serial")
	private static boolean spatialRangeHelper(String rectanglesFilePath, String queryWindowFilePath,
			String ouputFilePath, JavaSparkContext sc) {
		try {
			JavaRDD<String> recStr = sc.textFile(rectanglesFilePath);
			if (Settings.D)
				Utils.Log("Fetched Retangles");
			JavaRDD<String> qwStr = sc.textFile(queryWindowFilePath);
			if (Settings.D)
				Utils.Log("Fetched Query Window(s)");

			// Typecast Rectangles
			JavaRDD<Rectangle> rectangles = recStr.map(new Function<String, Rectangle>() {
				public Rectangle call(String s) {
					Float[] nums = Utils.splitStringToFloat(s, ",");
					if (nums.length != 5) {
						// The input dataset doesnt have ID as first column
						return new Rectangle(nums[0], nums[1], nums[2], nums[3]);
					}
					return new Rectangle(nums[0], nums[1], nums[2], nums[3], nums[4]);
				}
			});
			if (Settings.D)
				Utils.Log("Created Retangle Objects");
			if (Settings.D)
				Utils.Log("First Retangle: " + rectangles.first());

			// Typecast Query Window from String
			final JavaRDD<Rectangle> queryWindows = qwStr.map(new Function<String, Rectangle>() {
				public Rectangle call(String s) {
					Float[] nums = Utils.splitStringToFloat(s, ",");
					return new Rectangle(nums[0], nums[1], nums[2], nums[3]);
				}
			});
			if (Settings.D)
				Utils.Log("Created Query Window Retangle(s) Objects");
			final Rectangle query = queryWindows.first(); // Getting the first as we only deal with one window
			if (Settings.D)
				Utils.Log("First Query Window: " + query);

			// Filter out the rectangles which dont fall within the query window
			JavaRDD<Rectangle> resultRectangles = rectangles.filter(new Function<Rectangle, Boolean>() {
				public Boolean call(Rectangle r) {
					if (Settings.D)
						Utils.Log("Checking if [" + r + "] is inside [" + query + "] ");
					return query.isPointInside(r.getBottomLeft()) && query.isPointInside(r.getBottomRight())
							&& query.isPointInside(r.getTopLeft()) && query.isPointInside(r.getTopRight());
				}
			});
			if (Settings.D)
				Utils.Log("Filtered Retangles");

			resultRectangles.repartition(1).saveAsTextFile(ouputFilePath);
			if (Settings.D)
				Utils.Log("Saved the filtered Rectangles");

			// Now fetch the IDs of resultant rectangles and save to HDFS
			// resultRectangles.map(new Function<Rectangle, Integer>() {
			// public Integer call(Rectangle r) {
			// return (int) r.getId();
			// }
			// }).repartition(1).saveAsTextFile(ouputFilePath);
			// if (Settings.D)
			// Utils.Log("Saved the IDs of filtered Rectangles");

			Utils.Log("Done!");

			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

}
