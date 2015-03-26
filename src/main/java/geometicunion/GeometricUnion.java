package geometicunion;

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
import common.Rectangle;
import common.Settings;
import common.Utils;

/* 
 * Answer of the question 1 which calculate the geometry union of the given data set.  
 * */

public class GeometricUnion {

    public static void main(String[] args) {

        String inp = "union_inp1"; // in my HDFS
        String out = "union_out_" + Utils.getEpochTick();

        geometricUnion(inp, out);
    }

    public static boolean geometricUnion(String rectanglesFilePath,
            String ouputFilePath) {
        SparkConf conf = new SparkConf().setAppName("Geometric Union Module");
        JavaSparkContext sc = new JavaSparkContext(conf);
        boolean result = geometricUnionHelper(rectanglesFilePath,
                ouputFilePath, sc);

        sc.close();
        return result;
    }

    @SuppressWarnings("serial")
    private static boolean geometricUnionHelper(String rectanglesFilePath,
            String ouputFilePath, JavaSparkContext sc) {
        try {
            JavaRDD<String> recStr = sc.textFile(rectanglesFilePath);
            if (Settings.D)
                Utils.Log("Fetched Retangles");
            // JavaRDD<String> qwStr = sc.textFile(queryWindowFilePath);

            /*
             * if (Settings.D) Utils.Log("Fetched Query Window(s)");
             */

            // Typecast Rectangles

            JavaRDD<Rectangle> rectangles = recStr
                    .map(new Function<String, Rectangle>() {
                        public Rectangle call(String s) {
                            Float[] nums = Utils.splitStringToFloat(s, ",");
                            return new Rectangle(nums[0], nums[1], nums[2],
                                    nums[3]);
                        }
                    });

            if (Settings.D)
                Utils.Log("Created Retangle Objects");
            if (Settings.D)
                Utils.Log("First Retangle: " + rectangles.first());

            JavaRDD<Rectangle> rect = rectangles;

            /*
             * list of all the rectangles
             */

            final List<Rectangle> union = rect.collect();

            /*
             * Calculated all the points same as rectangle class
             */

            JavaRDD<Point> pointRDD = recStr.map(new Function<String, Point>() {
                public Point call(String s) {
                    Float[] nums = Utils.splitStringToFloat(s, ",");
                    return new Point(nums[0], nums[1], nums[2], nums[3]);

                }
            });

            List<Point> pointList = pointRDD.collect();

            long count = rect.count();

            /*
             * store the point which is inside the intersection of rectangles
             */

            JavaRDD<Point> intersectedRectangles = pointRDD
                    .filter(new Function<Point, Boolean>() {
                        public Boolean call(Point r) {
                            if (Settings.D)
                                Utils.Log("Checking if [" + r + "] is inside ["
                                        + union + "] ");
                            for (Rectangle rx : union)
                                return (rx).isPointInside(r);
                            return false;
                        }
                    });

            List<Point> intersectedList = intersectedRectangles.collect();

            pointList.removeAll(intersectedList);
            
            JavaRDD<Point> resultSet =  sc.parallelize(pointList);
            
            resultSet.saveAsTextFile(ouputFilePath);

            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

}

