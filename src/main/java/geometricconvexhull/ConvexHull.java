package geometricconvexhull;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import common.Point;
import common.Settings;
import common.Utils;

public class ConvexHull {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String inp = "convexhull_inpl"; // in HDFS
		String outp = "closest_out_" + Utils.getEpochTick();
		buildHull(inp, outp);

	}

	public static boolean buildHull(String rectanglesFilePath, String ouputFilePath) {

		SparkConf conf = new SparkConf().setAppName("Closest Pair Module");
		JavaSparkContext sc = new JavaSparkContext(conf);
		boolean result = quickHullHelper(rectanglesFilePath, ouputFilePath, sc);

		sc.close();
		return result;
	}

	private static boolean quickHullHelper(String pointsFilePath, String ouputFilePath, JavaSparkContext sc) {
		try {
			JavaRDD<String> pointStrings = sc.textFile(pointsFilePath);
			if (Settings.D)
				Utils.Log("Fetched Points");

			// Typecast Points
			JavaRDD<Point> pointsRDD = pointStrings.map(new Function<String, Point>() {
				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				public Point call(String s) {
					Float[] nums = Utils.splitStringToFloat(s, ",");
					return new Point(nums[0], nums[1]);
				}
			}).cache();
			if (Settings.D)
				Utils.Log("Created Point Objects");
			if (Settings.D)
				Utils.Log("First Point: " + pointsRDD.first());

			JavaRDD<List<Point>> listRDD = pointsRDD.glom();

			JavaRDD<List<Point>> pairs = listRDD.map(new Function<List<Point>, List<Point>>() {

				private static final long serialVersionUID = 1L;

				public List<Point> call(List<Point> points) throws Exception {
					// TODO Auto-generated method stub
					ArrayList<Point> convexHull = new ArrayList<Point>();
					if (points.size() < 3)
						return points;
					int minPoint = -1, maxPoint = -1;
					float minX = Integer.MAX_VALUE, maxX = Integer.MIN_VALUE;

					for (int i = 0; i < points.size(); i++) {
						Point pxy = points.get(i);
						if (pxy.getXcoordinate() < minX) {
							minX = pxy.getXcoordinate();
							minPoint = i;
						}
						if (pxy.getXcoordinate() > maxX) {
							maxX = pxy.getXcoordinate();
							maxPoint = i;
						}
					}

					Point A = new Point(points.get(minPoint).getXcoordinate(), points.get(minPoint).getYcoordinate());
					Point B = new Point(points.get(maxPoint).getXcoordinate(), points.get(maxPoint).getYcoordinate());

					convexHull.add(A);
					convexHull.add(B);
					points.remove(A);
					points.remove(B);

					ArrayList<Point> leftSet = new ArrayList<Point>();
					ArrayList<Point> rightSet = new ArrayList<Point>();

					for (int i = 0; i < points.size(); i++) {
						Point p = points.get(i);
						if (p.getDistance(A, B, p) == -1)
							leftSet.add(p);
						else
							rightSet.add(p);
					}

					hullSet(A, B, rightSet, convexHull);
					hullSet(A, B, leftSet, convexHull);

					return convexHull;
				}
			});
			if (Settings.D)
				Utils.Log("Convex Hull First" + pairs.first());

			pairs.map(new Function<List<Point>, String>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				public String call(List<Point> t) throws Exception {
					// TODO Auto-generated method stub
					return t.toString();
				}
			}).saveAsTextFile(ouputFilePath);

			Utils.Log("Done!");

			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	public static void hullSet(Point A, Point B, ArrayList<Point> set, ArrayList<Point> hull) {
		int insertPosition = hull.indexOf(B);

		if (set.size() == 0)
			return;

		if (set.size() == 1) {
			Point p = set.get(0);
			set.remove(p);
			hull.add(insertPosition, p);
			return;
		}

		float dist = Integer.MIN_VALUE;
		int furthestPoint = -1;

		for (int i = 0; i < set.size(); i++) {
			Point p = set.get(i);
			float distance = p.getDist(A, B, p);
			if (distance > dist) {
				dist = distance;
				furthestPoint = i;
			}
		}

		Point P = set.get(furthestPoint);
		set.remove(furthestPoint);
		hull.add(insertPosition, P);

		ArrayList<Point> leftSetAP = new ArrayList<Point>();
		for (int i = 0; i < set.size(); i++) {
			Point M = set.get(i);
			if (A.getDistance(A, P, M) == 1) {
				leftSetAP.add(M);
			}

		}

		ArrayList<Point> leftSetPB = new ArrayList<Point>();
		for (int i = 0; i < set.size(); i++) {
			Point M = set.get(i);
			if (M.getDistance(P, B, M) == 1)
				leftSetPB.add(M);
		}

		hullSet(A, P, leftSetAP, hull);
		hullSet(P, B, leftSetPB, hull);
	}
}
