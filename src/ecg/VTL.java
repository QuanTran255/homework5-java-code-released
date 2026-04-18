package ecg;

public class VTL {
	public final int v; // original value
	public final long ts; // timestamp
	public final double l; // value after applying the length transform

	public VTL(int v, long ts, double l) {
		this.v = v;
		this.ts = ts;
		this.l = l;
	}

	@Override
	public String toString() {
		String str = "{ v: " + v;
		str += ", ts: " + ts;
		str += ", l: " + l;
		str += " }";
		return str;
	}
}
