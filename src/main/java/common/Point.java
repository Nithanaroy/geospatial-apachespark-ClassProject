package common;

/**
 * A simple class which stores the schema of a Point with (X, Y) co-ordinates
 */
public class Point implements java.io.Serializable {

	/**
	 * A unique ID that helps during serialization and de-serialization of this class
	 */
	private static final long serialVersionUID = 8458302816992779299L;

	private float xcoordinate, ycoordinate;

	public Point(Float x, Float y) {
		xcoordinate = x;
		ycoordinate = y;
	}

	public float getXcoordinate() {
		return xcoordinate;
	}

	public float getYcoordinate() {
		return ycoordinate;
	}

	public float[] getVertices() {
		float[] f = { xcoordinate, ycoordinate };
		return f;
	}

	public float getDistance(Point p1) {
		// TODO: Take the distance function as argument
		return (float) Math.sqrt(Math.pow(p1.getXcoordinate() - this.getXcoordinate(), 2)
				+ Math.pow(p1.getYcoordinate() - this.getYcoordinate(), 2));
	}
	
	public float getDistance(Point A,Point B,Point P){
		float cp1 = (B.xcoordinate - A.xcoordinate) * (P.ycoordinate - A.ycoordinate) - (B.ycoordinate - A.ycoordinate) * (P.xcoordinate - A.xcoordinate);
		
		if(cp1 > 0)
			return 1;
		else if(cp1 == 0)
			return 0;
		else
			return -1;
	}
	
	public float getDist(Point A,Point B,Point P){
		float ABx = B.xcoordinate - A.xcoordinate;
		float ABy= B.ycoordinate - A.ycoordinate;
		
		float num = (ABx * (A.ycoordinate - P.ycoordinate) )- (ABy * (A.xcoordinate - P.xcoordinate));
		if ( num < 0)
			num = -num;
		return num;
	}
	
	public String asSimpleString() {
		return String.format("%s, %s", xcoordinate, ycoordinate);
	}

	public String toString() {
		return String.format("(%s, %s)", xcoordinate, ycoordinate);
	}
}
