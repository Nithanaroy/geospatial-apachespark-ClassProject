package spatialjoin;

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
import common.Rectangle;
import common.Settings;
import common.Utils;

/**
 * Answer for Question 6 which filters the polygons which completely fall inside
 * a query windows provided CHECKING AGAINST MULTIPLE QUERY WINDOWS. LOOPING THE
 * RANGE FUNCTION.
 */
public class HeatMap {

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

		// String inp1 = "join_inp1"; // in my HDFS
		// String inp2 = "join_inp2"; // in my HDFS
		// String out = "join_out_" + Utils.getEpochTick();
		String inp1 = args[0]; // in my HDFS
		String inp2 = args[1]; // in my HDFS
		int partitions = -1;
		try {
			partitions = Integer.parseInt(args[2]);
		} catch (Exception e) {
		}
		String out = "join_out_" + Utils.getEpochTick();

		spatialJoin(inp1, inp2, out, partitions);

	}

	/**
	 * The wrapper for Spatial Range Helper. This is the function which user
	 * will call.
	 * 
	 * @param rectanglesFilePath
	 *            Input file path having all rectangles
	 * @param queryWindowFilePath
	 *            Input file path in HDFS having the query window
	 * @param ouputFilePath
	 *            Path in HDFS where to save the output
	 * @return true if everything is successful, else false
	 */
	public static boolean spatialJoin(String rectanglesFilePath, String queryWindowFilePath, String ouputFilePath, int partitions) {
		SparkConf conf = new SparkConf().setAppName("Spatial Join Module");
		JavaSparkContext sc = new JavaSparkContext(conf);
		boolean result = spatialJoinHelper(rectanglesFilePath, queryWindowFilePath, ouputFilePath, partitions, sc);

		sc.close();
		return result;
	}

	/**
	 * The helper method which performs all computation for Spatial Join query
	 * 1) Fetches the rectangles from inp1 in HDFS 2) Fetches the query
	 * window(s) from inp2 in HDFS 3) Type casts rectangles and query windows to
	 * Rectangle class 4) Filters only those rectangles whose all corners lie
	 * within the query window 5) Saves the IDs of those filtered rectangles to
	 * HDFS
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
	private static boolean spatialJoinHelper(String rectanglesFilePath, String queryWindowFilePath, String ouputFilePath, int partitions, JavaSparkContext sc) {
		try {
			JavaRDD<String> recStr = null;
			recStr = sc.textFile(rectanglesFilePath);

			if (Settings.D)
				Utils.Log("Fetched Retangles");
			JavaRDD<String> qwStr = sc.textFile(queryWindowFilePath);
			if (Settings.D)
				Utils.Log("Fetched Query Window(s)");

			/*
			 * JavaRDD<String> recStr = sc.textFile(rectanglesFilePath); if
			 * (Settings.D) Utils.Log("Fetched Retangles"); JavaRDD<String>
			 * qwStr = sc.textFile(queryWindowFilePath); if (Settings.D)
			 * Utils.Log("Fetched Query Window(s)");
			 */
			// Typecast Rectangles
			JavaPairRDD<Rectangle, Long> rectangles = getRectangleObjects(recStr);
			if (Settings.D)
				Utils.Log("Created Retangle Objects");

			/*
			 * if (Settings.D) Utils.Log("Created Retangle Objects"); if
			 * (Settings.D) Utils.Log("First Retangle: " + rectangles.first());
			 */
			// Typecast Query Window from String
			JavaPairRDD<Rectangle, Long> query = getRectangleObjects(qwStr);
			if (Settings.D)
				Utils.Log("Created Query Window Rectangle Objects");

			final Broadcast<List<Tuple2<Rectangle, Long>>> brect = sc.broadcast(rectangles.collect());
			// Filter out the rectangles which don't fall within the query
			// window

			JavaPairRDD<Tuple2<Rectangle, Long>, Long> resultRectangles = query.mapToPair(new PairFunction<Tuple2<Rectangle, Long>, Tuple2<Rectangle, Long>, Long>() {
				public Tuple2<Tuple2<Rectangle, Long>, Long> call(Tuple2<Rectangle, Long> s) {
					Rectangle r = s._1();
					List<Tuple2<Rectangle, Long>> rect = brect.getValue();
					long count = 0;
					for (final Tuple2<Rectangle, Long> rxy1 : rect) {
						Rectangle rxy = rxy1._1();
						if (rxy.isPointInside(r.getBottomLeft()) && rxy.isPointInside(r.getBottomRight()) && rxy.isPointInside(r.getTopLeft()) && rxy.isPointInside(r.getTopRight()))
							count += rxy1._2();
					}
					return new Tuple2<Tuple2<Rectangle, Long>, Long>(s, count);
				}
			});

			if (Settings.D)
				Utils.Log("Filtered Retangles");

			resultRectangles.saveAsTextFile(ouputFilePath);

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

		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	@SuppressWarnings("serial")
	private static JavaPairRDD<Rectangle, Long> getRectangleObjects(JavaRDD<String> recStr) {
		return recStr.mapToPair(new PairFunction<String, Rectangle, Long>() {
			public Tuple2<Rectangle, Long> call(String s) {
				Rectangle r = null;
				Float[] nums = Utils.splitStringToFloat(s, ",");
				switch (nums.length) {
				case 18:
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
					// throw new
					// IllegalArgumentException("Unknown Schema");
					// Ignore lines when schema is unknown
				}
				return new Tuple2<Rectangle, Long>(r, 1L);
			}
		}).filter(new Function<Tuple2<Rectangle, Long>, Boolean>() {
			@Override
			public Boolean call(Tuple2<Rectangle, Long> t) throws Exception {
				return t._1() != null;
			}

		}).reduceByKey(new Function2<Long, Long, Long>() {
			public Long call(Long r1, Long r2) throws Exception {
				return r1 + r2;
			}
		});
	}
}
