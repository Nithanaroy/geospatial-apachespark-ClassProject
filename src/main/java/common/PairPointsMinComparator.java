package common;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class PairPointsMinComparator<String, PairPoints> implements Comparator<Tuple2<String, PairPoints>>,
		Serializable {

	/**
	 * A unique ID that helps during serialization and de-serialization of this class
	 */
	private static final long serialVersionUID = 1883709778800078990L;

	// public int compare(PairPoints pts1, PairPoints pts2) {
	// // If points in pts1 are closer than points in pts2, then pts1 is less than pts2
	// return (int) (pts1.getDistance() - pts2.getDistance());
	// }

	private Comparator<PairPoints> comparator;

	public PairPointsMinComparator(Comparator<PairPoints> comparator) {
		this.comparator = comparator;
	}

	// @Override
	// public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
	// return comparator.compare(o1._2(), o2._2());
	// }

	public int compare(Tuple2<String, PairPoints> pts1, Tuple2<String, PairPoints> pts2) {
		// If points in pts1 are closer than points in pts2, then pts1 is less than pts2
		// return (int) (((PairPoints) pts1._2()).getDistance() - pts2.getDistance());
		return comparator.compare(pts1._2(), pts2._2());
	}

}
