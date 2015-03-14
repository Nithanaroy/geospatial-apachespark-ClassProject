/*
 * Author: Nitin Pasumarthy
 * 
 * Source: https://spark.apache.org/docs/latest/quick-start.html  > Self Contained Applications 
 * Pre-Conditions: Hadoop is running using ./start-dfs, Mentioned file below "data.txt" should be in HDFS
 * Instructions to run:
 * 1) mvn clean package   // Exports a jar file called geospatial-spark-0.0.1-SNAPSHOT.jar in the target directory
 * 2) ./dev/spark/bin/spark-submit --class "spatialrange.SimpleApp" --master local[4] geospatial-apachespark/target/geospatial-spark-0.0.1-SNAPSHOT.jar
 * 
 * Date Created: Mar 10, 2015
 */

package spatialrange;

/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class SimpleApp {
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		// String logFile = "/home/hduser/dev/geospatial-apachespark/README.md";
		String logFile = "data.txt"; // in my HDFS
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(logFile).cache();

		long numAs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("a");
			}
		}).count();

		long numBs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.contains("b");
			}
		}).count();

		System.out.println("Lines with a: " + numAs + ", lines with b: "
				+ numBs);

		sc.close();
	}
}
