package common;

public class PairPoints implements java.io.Serializable {

	/**
	 * A unique ID that helps during serialization and de-serialization of this class
	 */
	private static final long serialVersionUID = -6567403437787255045L;
	private Point p1, p2;
	private float distance;

	public PairPoints(Point p1, Point p2, float distance) {
		super();
		this.p1 = p1;
		this.p2 = p2;
		this.distance = distance;
	}

	public float getDistance() {
		return distance;
	}

	public void setDistance(float distance) {
		this.distance = distance;
	}

	public String toString() {
		return String.format("%s and %s", p1, p2);
	}

}
