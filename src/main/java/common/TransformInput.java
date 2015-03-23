/**
 * Transform the given input schema to the one which each function is expecting
 * Instructions to run:
 * 1) mvn clean package   // Exports a jar file called geospatial-spark-0.0.1-SNAPSHOT.jar in the target directory
 * 2) ./dev/spark/bin/spark-submit --class "spatialrange.TranformInput" --master local[2] geospatial-apachespark/target/geospatial-spark-0.0.1-SNAPSHOT.jar range1 home/range1_inp.txt home/range1_out.txt
 * 
 * Date Created: Mar 22, 2015
 */
package common;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * @author Nitin Pasumarthy
 */
public class TransformInput {

	/**
	 * @param args
	 *            tranformation_for_token inputpath outputpath
	 */
	public static void main(String[] args) {
		if (args.length != 3) {
			System.out
					.println("Three arguments are required:\n tranformation_token (union, hull, farthest, closest, range1, range2, join1, join2),\n inputfilepath,\n outputfilepath");
			System.exit(0);
		}
		String token = args[1];
		String inputFilePath = args[2];
		String outputFilePath = args[3];

		switch (token) {
		case "union":
			// TODO
			break;
		case "hull":
			// TODO
			break;
		case "closest":
			forClosestPair(inputFilePath, outputFilePath);
			break;
		case "farthest":
			forFarthestPair(inputFilePath, outputFilePath);
			break;
		case "range1":
			forSpatialRangeInp1(inputFilePath, outputFilePath);
			break;
		case "range2":
			forSpatialRangeInp2(inputFilePath, outputFilePath);
			break;
		case "join1":
			// TODO
			break;
		case "join2":
			// TODO
			break;

		default:
			System.out.println("Unknown token specified");
			System.exit(0);
			break;
		}
	}

	/**
	 * Tranformation for Input1 for Spatial Range
	 * 
	 * @param inputFilePath
	 *            Path for input data file
	 * @param outputFilePath
	 *            Path for output data file
	 * @return true if successful, else false
	 */
	public static boolean forSpatialRangeInp1(String inputFilePath, String outputFilePath) {
		/* 0000100000001030000000100000006000000AEB8382A371556C0D4298F6E84294040176536C8241556C08B15359886294040BD19355F251556C0191D90847D294040B4A9BA47361556C049A12C7C7D29404049F3C7B4361556C0CA17B49080294040AEB8382A371556C0D4298F6E84294040,1,-88.331492,32.324142,-88.33037,32.32442,01,,11081154767,,K2582,2755,0,+32.3242672,-88.3309042 */
		/* '1', '-88.331492', '32.324142', '-88.33037', '32.32442' */

		SparkConf conf = new SparkConf().setAppName("Spatial Range Tranform Input1 Module");
		JavaSparkContext sc = new JavaSparkContext(conf);

		try {
			JavaRDD<String> lines = sc.textFile(inputFilePath);
			if (Settings.D)
				Utils.Log("Fetched Lines");

			saveSubstringOfLine(outputFilePath, lines, new int[] { 1, 2, 3, 4, 5 });

			Utils.Log("Done");

			return true;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			sc.close();
		}
		return false;
	}

	/**
	 * Tranformation for Input2 for Spatial Range
	 * 
	 * @param inputFilePath
	 *            Path for input data file
	 * @param outputFilePath
	 *            Path for output data file
	 * @return true if successful, else false
	 */
	public static boolean forSpatialRangeInp2(String inputFilePath, String outputFilePath) {
		/* 0000100000001030000000100000006000000AEB8382A371556C0D4298F6E84294040176536C8241556C08B15359886294040BD19355F251556C0191D90847D294040B4A9BA47361556C049A12C7C7D29404049F3C7B4361556C0CA17B49080294040AEB8382A371556C0D4298F6E84294040,1,-88.331492,32.324142,-88.33037,32.32442,01,,11081154767,,K2582,2755,0,+32.3242672,-88.3309042 */
		/* '1', '-88.331492', '32.324142', '-88.33037', '32.32442' */

		SparkConf conf = new SparkConf().setAppName("Spatial Range Tranform Input2 Module");
		JavaSparkContext sc = new JavaSparkContext(conf);

		try {
			JavaRDD<String> lines = sc.textFile(inputFilePath);
			if (Settings.D)
				Utils.Log("Fetched Lines");

			saveSubstringOfLine(outputFilePath, lines, new int[] { 2, 3, 4, 5 });

			Utils.Log("Done");

			return true;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			sc.close();
		}
		return false;
	}

	/**
	 * Tranformation for Input for Closest Pair
	 * 
	 * @param inputFilePath
	 *            Path for input data file
	 * @param outputFilePath
	 *            Path for output data file
	 * @return true if successful, else false
	 */
	public static boolean forClosestPair(String inputFilePath, String outputFilePath) {
		/* 0000100000001030000000100000006000000AEB8382A371556C0D4298F6E84294040176536C8241556C08B15359886294040BD19355F251556C0191D90847D294040B4A9BA47361556C049A12C7C7D29404049F3C7B4361556C0CA17B49080294040AEB8382A371556C0D4298F6E84294040,1,-88.331492,32.324142,-88.33037,32.32442,01,,11081154767,,K2582,2755,0,+32.3242672,-88.3309042 */
		/* '1', '-88.331492', '32.324142', '-88.33037', '32.32442' */

		SparkConf conf = new SparkConf().setAppName("Closest Pair Tranform Input Module");
		JavaSparkContext sc = new JavaSparkContext(conf);

		try {
			JavaRDD<String> lines = sc.textFile(inputFilePath);
			if (Settings.D)
				Utils.Log("Fetched Lines");

			saveSubstringOfLine(outputFilePath, lines, new int[] { 2, 3 });
			// saveSubstringOfLine(outputFilePath, lines, new int[] { 4, 5 }); // Can be this also

			Utils.Log("Done");

			return true;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			sc.close();
		}
		return false;
	}

	/**
	 * Tranformation for Input for Farthest Pair
	 * 
	 * @param inputFilePath
	 *            Path for input data file
	 * @param outputFilePath
	 *            Path for output data file
	 * @return true if successful, else false
	 */
	public static boolean forFarthestPair(String inputFilePath, String outputFilePath) {
		/* 0000100000001030000000100000006000000AEB8382A371556C0D4298F6E84294040176536C8241556C08B15359886294040BD19355F251556C0191D90847D294040B4A9BA47361556C049A12C7C7D29404049F3C7B4361556C0CA17B49080294040AEB8382A371556C0D4298F6E84294040,1,-88.331492,32.324142,-88.33037,32.32442,01,,11081154767,,K2582,2755,0,+32.3242672,-88.3309042 */
		/* '1', '-88.331492', '32.324142', '-88.33037', '32.32442' */

		return forClosestPair(inputFilePath, outputFilePath);
	}

	/**
	 * Extracts the required columns of CSV lines passed as indices
	 * 
	 * @param outputFilePath
	 *            Path of output where to save extracted data
	 * @param lines
	 *            Raw lines in which sub part of each line has to be extracted
	 * @param pieceIndices
	 *            Zero based indices of line
	 */
	@SuppressWarnings("serial")
	public static void saveSubstringOfLine(String outputFilePath, JavaRDD<String> lines, final int[] pieceIndices) {
		lines.map(new Function<String, String>() {
			public String call(String s) {
				String[] pieces = s.split(",");
				String line = "";
				for (int i : pieceIndices) {
					line += pieces[i] + ",";
				}
				return line.substring(0, line.length() - 1); // remove the trailing comma
			}
		}).saveAsTextFile(outputFilePath);
	}

}
