package geometricfarthestpoints;

import common.Point;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import common.Settings;
import common.Utils;


public class FarthestPair implements Serializable {
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) {

		//String inp1 = "hdfs://master:54310/user/hduser/arealm.csv"; // in my HDFS
		//String out = "hdfs://master:54310/user/hduser/farthest_out_" + Utils.getEpochTick();
		String inp1 = args[0];
		String out = args[1]+Utils.getEpochTick();
		int partitions = -1;
		try {
			partitions = Integer.parseInt(args[2]);
		} catch (Exception e) {
		}
		buildHull(inp1, out, partitions);
	}
	
	public static boolean buildHull(String rectanglesFilePath, String ouputFilePath, int partitions) {

		SparkConf conf = new SparkConf().setAppName("Farthest Point Module");
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kyro.registrationRequired", "true");
		JavaSparkContext sc = new JavaSparkContext(conf);
		boolean result = quickHullHelper(rectanglesFilePath, ouputFilePath, sc, partitions);

		sc.close();
		return result;
	}

	@SuppressWarnings("serial")
	private static boolean quickHullHelper(String pointsFilePath, String ouputFilePath, JavaSparkContext sc, int partitions) {
		try {
			JavaRDD<String> pointStrings = sc.textFile(pointsFilePath);
			
			JavaRDD<Point> pointsRDD = pointStrings.map(new Function<String, Point>() {
				private static final long serialVersionUID = 1L;

				public Point call(String s) {
					
					Point p = null;
					Float[] nums = Utils.splitStringToFloat(s, ",");
					
					switch (nums.length) {
					case 18:
						// schema 1
						p = new Point(nums[2], nums[3]);
						break;
					
					case 15:
						// schema 2
						p = new Point(nums[2], nums[3]);
						break;

					case 5:
						// schema 3
						p = new Point(nums[1], nums[2]);
						break;

					case 4:
						// schema 4
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

			JavaRDD<List<Point>> listRDD = pointsRDD.glom();

			JavaRDD<List<Point>> pairs = listRDD.map(new Function<List<Point>, List<Point>>() {

				private static final long serialVersionUID = 1L;

				public List<Point> call(List<Point> points) throws Exception {
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
							maxX = pxy.getYcoordinate();
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
						else if (p.getDistance(A, B, p) == 1)
							rightSet.add(p);
					}

					hullSet(A, B, rightSet, convexHull);
					hullSet(B, A, leftSet, convexHull);

					return convexHull;
				}

			});
			if (Settings.D)
				Utils.Log("Convex Hull First" + pairs.first());
			
			pairs.map(new Function<List<Point>, String>() {

				public String call(List<Point> t) throws Exception {
					String s = "";
					List<Point> fp=new ArrayList<Point>();
					
					if(!t.isEmpty()) {
						Point p1, p2;
						p1 = t.get(0);
						p2 = t.get(1);
						
						double maxDistance=0;
						int convexHullSize = t.size();
						
						
						for(int i=0;i<convexHullSize-1;i++)
						{
							for(int j=i+1;j<convexHullSize;j++)
							{
								double xsquare=(t.get(i).getXcoordinate()-t.get(j).getXcoordinate())*(t.get(i).getXcoordinate()-t.get(j).getXcoordinate());
								double ysquare=(t.get(i).getYcoordinate()-t.get(j).getYcoordinate())*(t.get(i).getYcoordinate()-t.get(j).getYcoordinate());
								double currentDistance=Math.sqrt(xsquare+ysquare);	

								if(currentDistance>maxDistance)
								{
									maxDistance=currentDistance;
									p1=t.get(i);
									p2=t.get(j);
								}
							}	
						}
						fp.add(p1);
						fp.add(p2);
					}
					
					if(!fp.isEmpty()){
					String str = fp.toString(); // [(3,4),(5,6)] => 3,4\n5,6
					str = str.substring(1, str.length() - 1); // => (3,4),(5,6)
					String[] splitstr = str.split(","); // Array of (3,4) (5,6)
					for (int i = 0; i < splitstr.length; i = i + 2) {
						if (i == 0) {
							s = s + splitstr[i].substring(1, splitstr[i].length() - 1) + ", "
									+ splitstr[i + 1].substring(2, splitstr[i + 1].length() - 1) + "\n";
						} else {
							s = s + splitstr[i].substring(2, splitstr[i].length() - 1) + ", "
									+ splitstr[i + 1].substring(2, splitstr[i + 1].length() - 1) + "\n";

						}
					}
					
					}
					return s;
				}
			}).repartition(partitions).saveAsTextFile(ouputFilePath);

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
