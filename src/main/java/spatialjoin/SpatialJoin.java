package spatialjoin;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import common.Point;
import common.Rectangle;
import common.Settings;
import common.Utils;

/**
 * Answer for Question 6 which filters the polygons which completely fall inside a query windows provided CHECKING AGAINST MULTIPLE QUERY WINDOWS. LOOPING THE RANGE FUNCTION.
 */
public class SpatialJoin {

	/**
	 * A dummy tester for Spatial Join
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// String inp1 =
		// "/home/hduser/dev/geospatial-apachespark/data/range_inp1";
		// String inp2 =
		// "/home/hduser/dev/geospatial-apachespark/data/range_inp1";

		String inp1 = "join_inp1"; // in my HDFS
		String inp2 = "join_inp2"; // in my HDFS
		String out = "join_out_" + Utils.getEpochTick();

		spatialJoin(inp1, inp2, out);

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
	public static boolean spatialJoin(String rectanglesFilePath, String queryWindowFilePath, String ouputFilePath) {
		SparkConf conf = new SparkConf().setAppName("Spatial Join Module");
		JavaSparkContext sc = new JavaSparkContext(conf);
		boolean result = spatialJoinHelper(rectanglesFilePath, queryWindowFilePath, ouputFilePath, sc);

		sc.close();
		return result;
	}

	/**
	 * The helper method which performs all computation for Spatial Join query 1) Fetches the rectangles from inp1 in HDFS 2) Fetches the query window(s) from inp2 in HDFS 3) Type casts rectangles and query windows to Rectangle class 4) Filters only those rectangles whose all corners lie within the query window 5) Saves the IDs of those filtered rectangles to HDFS
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
	private static boolean spatialJoinHelper(String rectanglesFilePath, String queryWindowFilePath,
			String ouputFilePath, JavaSparkContext sc) {
		try {
			JavaRDD<String> recStr = sc.textFile(rectanglesFilePath);
			if (Settings.D)
				Utils.Log("Fetched Retangles");
			JavaRDD<String> qwStr = sc.textFile(queryWindowFilePath);
			if (Settings.D)
				Utils.Log("Fetched Query Window(s)");

			// Typecast Rectangles
			final JavaRDD<Rectangle> rectangles = recStr.map(new Function<String, Rectangle>() {
				public Rectangle call(String s) {
					Float[] nums = Utils.splitStringToFloat(s, ",");
					return new Rectangle(nums[0], nums[1], nums[2], nums[3]);
				}
			});
			if (Settings.D)
				Utils.Log("Created Retangle Objects");
			if (Settings.D)
				Utils.Log("First Retangle: " + rectangles.first());

			// Typecast Query Window from String
			final JavaRDD<Rectangle> query = qwStr.map(new Function<String, Rectangle>() {
				public Rectangle call(String s) {
					Float[] nums = Utils.splitStringToFloat(s, ",");
					return new Rectangle(nums[0], nums[1], nums[2], nums[3]);
				}
			});
			if (Settings.D)
				Utils.Log("Created Query Window Rectangle(s) Objects");

			// final JavaRDD<Rectangle> queryWindows = qwStr.map(new Function<String, Rectangle>() {
			// public Rectangle call(String s) {
			// Float[] nums = Utils.splitStringToFloat(s, ",");
			// return new Rectangle(nums[0], nums[1], nums[2], nums[3]);
			// }
			// });

			// final List<Rectangle> rect = rectangles.collect();
			final List<Rectangle> quer = query.collect();
			// Filter out the rectangles which don't fall within the query window

			for (final Rectangle rxy : quer) {

				JavaRDD<Rectangle> resultRectangles = rectangles.filter(new Function<Rectangle, Boolean>() {
					public Boolean call(Rectangle r) {
						if (Settings.D)
							Utils.Log("Checking if [" + r + "] is inside [" + query + "] ");
						return rxy.isPointInside(r.getBottomLeft()) && rxy.isPointInside(r.getBottomRight())
								&& rxy.isPointInside(r.getTopLeft()) && rxy.isPointInside(r.getTopRight());
					}

				});
				if (Settings.D)
					Utils.Log("Filtered Retangles");

				resultRectangles.repartition(1).saveAsTextFile(ouputFilePath);

				// Now fetch the IDs of resultant rectangles and save to HDFS
				// resultRectangles.map(new Function<Rectangle, Integer>() {
				// public Integer call(Rectangle r) {
				// return (int) r.getId();
				// }
				// }).saveAsTextFile(ouputFilePath);
				// if (Settings.D)
				// Utils.Log("Saved the IDs of filtered Rectangles");

				Utils.Log("Done!");

				return true;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
}
