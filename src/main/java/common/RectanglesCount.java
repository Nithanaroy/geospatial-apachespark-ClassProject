package common;

public class RectanglesCount {

	private Rectangle r;
	private long count;

	public RectanglesCount(Rectangle r, long count) {
		super();
		this.r = r;
		this.count = count;
	}

	public Rectangle getR() {
		return r;
	}

	public long getCount() {
		return count;
	}

	public void setR(Rectangle r) {
		this.r = r;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return r.getBottomLeft() + ", " + r.getTopRight() + ": " + count;
	}

}
