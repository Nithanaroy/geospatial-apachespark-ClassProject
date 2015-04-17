/*
 * Author: Nitin Pasumarthy
 * 
 * Pre-Conditions: Hadoop is running using ./start-dfs, Mentioned files paths are valid in HDFS
 * Instructions to run:
 * 1) mvn clean package   // Exports a jar file called geospatial-spark-0.0.1-SNAPSHOT.jar in the target directory
 * 2) ./dev/spark/bin/spark-submit --class "spatialrange.SpatialRange" --master local[4] geospatial-apachespark/target/geospatial-spark-0.0.1-SNAPSHOT.jar inp1 inp2 [4]
 * 
 * Date Created: Mar 13, 2015
 */

package spatialrange;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

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

		// String inp1 = "range_inp1"; // in my HDFS
		// String inp2 = "range_inp2"; // in my HDFS

		String inp1 = args[0]; // in my HDFS
		String inp2 = args[1]; // in my HDFS
		int partitions = -1;
		try {
			partitions = Integer.parseInt(args[2]);
		} catch (Exception e) {
		}
		String out = "range_out_" + Utils.getEpochTick();

		spatialRange(inp1, inp2, out, partitions);

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
	public static boolean spatialRange(String rectanglesFilePath, String queryWindowFilePath, String ouputFilePath,
			int partitions) {
		SparkConf conf = new SparkConf().setAppName("Spatial Range Module");
		// conf.setMaster("spark://192.168.0.10:7077"); // Required to run from within eclipse
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kyro.registrationRequired", "true");
		JavaSparkContext sc = new JavaSparkContext(conf);
		boolean result = spatialRangeHelper(rectanglesFilePath, queryWindowFilePath, ouputFilePath, partitions, sc);

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
			String ouputFilePath, int partitions, JavaSparkContext sc) {
		try {
			JavaRDD<String> recStr = null;
			if (partitions > 0) {
				recStr = sc.textFile(rectanglesFilePath).repartition(partitions);
			} else {
				recStr = sc.textFile(rectanglesFilePath);
			}

			if (Settings.D)
				Utils.Log("Fetched Retangles");
			JavaRDD<String> qwStr = sc.textFile(queryWindowFilePath);
			if (Settings.D)
				Utils.Log("Fetched Query Window(s)");

			// Typecast Rectangles
			// schema 1: geom,gid,x1,y1,x2,y2,statefp,countyfp,ansicode,hydroid,fullname,mtfcc,aland,awater,intptlat,intptlon
			// schema 2: geom,gid,x1,y1,x2,y2,statefp,ansicode,areaid,fullname,mtfcc,aland,awater,intptlat,intptlon
			// schema 3: id,x1,y1,x2,y2
			// schema 4: x1,y1,x2,y2
			JavaRDD<Rectangle> rectangles = recStr.map(new Function<String, Rectangle>() {
				public Rectangle call(String s) {
					Rectangle r = null;
					Float[] nums = Utils.splitStringToFloat(s, ",");
					switch (nums.length) {
					case 16:
						// schema 1
						if (Settings.D)
							Utils.Log("Detected Schema 1");
						r = new Rectangle(nums[2], nums[3], nums[4], nums[5]);
						break;

					case 15:
						// schema 2
						if (Settings.D)
							Utils.Log("Detected Schema 2");
						r = new Rectangle(nums[2], nums[3], nums[4], nums[5]);
						break;

					case 5:
						// schema 3
						if (Settings.D)
							Utils.Log("Detected Schema 3");
						r = new Rectangle(nums[0], nums[1], nums[2], nums[3], nums[4]);
						break;

					case 4:
						// schema 4
						if (Settings.D)
							Utils.Log("Detected Schema 4");
						r = new Rectangle(nums[0], nums[1], nums[2], nums[3]);
						break;

					default:
						// unknown schema
						throw new IllegalArgumentException("Unknown Schema");
					}
					return r;
				}
			});
			if (Settings.D)
				Utils.Log("Created Retangle Objects");
			// Requires a new job
			// if (Settings.D)
			// Utils.Log("First Retangle: " + rectangles.first());

			// Typecast Query Window from String
			final JavaRDD<Rectangle> queryWindows = qwStr.map(new Function<String, Rectangle>() {
				public Rectangle call(String s) {
					Float[] nums = Utils.splitStringToFloat(s, ",");
					return new Rectangle(nums[0], nums[1], nums[2], nums[3]);
				}
			});
			if (Settings.D)
				Utils.Log("Created Query Window Retangle(s) Objects");

			// final Broadcast<Rectangle> query = sc.broadcast(queryWindows.first());
			// if (Settings.D)
			// Utils.Log("First Query Window: " + query.value());

			final Rectangle query = queryWindows.first(); // Getting the first as we only deal with one window
			if (Settings.D)
				Utils.Log("First Query Window: " + query);

			// Filter out the rectangles which dont fall within the query window
			JavaRDD<Rectangle> resultRectangles = rectangles.filter(new Function<Rectangle, Boolean>() {
				public Boolean call(Rectangle r) {
					// Rectangle queryRect = query.value(); // For Broadcast
					Rectangle queryRect = query;
					if (Settings.D)
						Utils.Log("Checking if [" + r + "] is inside [" + queryRect + "] ");
					return queryRect.isPointInside(r.getBottomLeft()) && queryRect.isPointInside(r.getBottomRight())
							&& queryRect.isPointInside(r.getTopLeft()) && queryRect.isPointInside(r.getTopRight());
				}
			});
			if (Settings.D)
				Utils.Log("Filtered Retangles");

			String debugString = resultRectangles.toDebugString();
			if (Settings.D)
				Utils.Log(debugString);

			resultRectangles.saveAsTextFile(ouputFilePath);

			// Now fetch the IDs of resultant rectangles and save to HDFS
			// resultRectangles.map(new Function<Rectangle, Integer>() {
			// public Integer call(Rectangle r) {
			// return (int) r.getId();
			// }
			// }).saveAsTextFile(ouputFilePath);
			if (Settings.D)
				Utils.Log("Saved the IDs of filtered Rectangles");

			Utils.Log("Done!");

			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

}
