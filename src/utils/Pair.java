package utils;

public class Pair<A, B> {

	private final A left;
	private final B right;

	private Pair(A left, B right) {
		this.left = left;
		this.right = right;
	}

	public static <A,B> Pair<A,B> from(A left, B right) {
		return new Pair<>(left, right);
	}

	public A getLeft() {
		return left;
	}

	public B getRight() {
		return right;
	}

	@Override
	public String toString() {
		return "(" + left.toString() + ", " + right.toString() + ")";
	}

}
