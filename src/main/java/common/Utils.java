package common;

import java.sql.Timestamp;

public class Utils {

	/**
	 * Helper to log anything to stdout
	 * 
	 * @param message
	 *            Message to log onto the stdout
	 */
	public static void Log(String message) {
		System.out.println("\nL: " + message + "\n");
	}

	/**
	 * Takes an input in the form of a string and converts into to float array based on a separator "(1,2.4,3)" will tranformed to [1F, 2.4F, 3F]
	 * 
	 * @param s
	 *            String to split
	 * @param separator
	 *            Separator to split on
	 * @return an array of floats after splitting
	 */
	public static Float[] splitStringToFloat(String s, String separator) {
		String[] splitString = s.split(separator);
		Float[] f = new Float[splitString.length];

		for (int i = 0; i < splitString.length; i++) {
			f[i] = Float.parseFloat(splitString[i]);
		}
		return f;
	}

	/**
	 * Gets the current timestamp in W3C Time format
	 * 
	 * @return
	 */
	public static String getCurrentTimeStamp() {
		java.util.Date date = new java.util.Date();
		return new Timestamp(date.getTime()).toString();
	}

	/**
	 * Gets the milliseconds from Epoch time. Useful if we want to generate unique names.
	 * 
	 * @return
	 */
	public static long getEpochTick() {
		return System.currentTimeMillis();
	}

}
