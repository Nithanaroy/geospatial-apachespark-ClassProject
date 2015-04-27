package common;

public class Rectangle implements java.io.Serializable {

	/**
	 * A unique ID that helps during serialization and de-serialization of this class
	 */
	private static final long serialVersionUID = -84411340410615281L;

	private float id = -1;

	private Point bottomLeft, bottomRight, topRight, topLeft;

	public Rectangle(float bottomLeftX, float bottomLeftY, float topRightX,
			float topRightY) {
		bottomLeft = new Point(bottomLeftX, bottomLeftY);
		topRight = new Point(topRightX, topRightY);

		bottomRight = new Point(topRightX, bottomLeftY);
		topLeft = new Point(bottomLeftX, topRightY);
	}

	public Rectangle(float id, float bottomLeftX, float bottomLeftY,
			float topRightX, float topRightY) {
		this.id = id;

		bottomLeft = new Point(bottomLeftX, bottomLeftY);
		topRight = new Point(topRightX, topRightY);

		bottomRight = new Point(topRightX, bottomLeftY);
		topLeft = new Point(bottomLeftX, topRightY);
	}

	public Point getBottomLeft() {
		return bottomLeft;
	}

	public Point getBottomRight() {
		return bottomRight;
	}

	public Point getTopRight() {
		return topRight;
	}

	public Point getTopLeft() {
		return topLeft;
	}

	/**
	 * Checks if the given point falls inside this rectangle
	 * @param p Point to check
	 * @return true if point lies inside, else false
	 */
	public boolean isPointInside(Point p) {
		float px = p.getXcoordinate(), py = p.getYcoordinate();
		return px > this.bottomLeft.getXcoordinate()
				&& py > this.bottomLeft.getYcoordinate()

				&& px < this.bottomRight.getXcoordinate()
				&& py > this.bottomRight.getYcoordinate()

				&& px < this.topRight.getXcoordinate()
				&& py < this.topRight.getYcoordinate()

				&& px > this.topLeft.getXcoordinate()
				&& py < this.topLeft.getYcoordinate();
	}

	/**
	 * Collects all vertices of the rectangle and returns in form of an array of floating numbers
	 * The vertices are listed from bottom left corner in anti clockwise direction to top left.
	 * @return
	 */
	public Float[] getFalttenedVertices() {
		Float[] f = new Float[8];
		f[0] = bottomLeft.getXcoordinate();
		f[1] = bottomLeft.getYcoordinate();

		f[2] = bottomRight.getXcoordinate();
		f[3] = bottomRight.getYcoordinate();

		f[4] = topRight.getXcoordinate();
		f[5] = topRight.getYcoordinate();

		f[6] = topLeft.getXcoordinate();
		f[7] = topLeft.getYcoordinate();
		return f;
	}

	public float getId() {
		return id;
	}

	public String toString() {
		return bottomLeft + ", " + bottomRight + ", " + topRight + ", " + topLeft;
	}
}
