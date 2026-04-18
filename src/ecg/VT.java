package ecg;

public class VT {
	public final int v;
	public final long ts;

	public VT(int v, long ts) {
		this.v = v;
		this.ts = ts;
	}

	public VTL extendl(double l) {
		return new VTL(v, ts, l);
	}

	@Override
	public String toString() {
		String str = "{ v: " + v;
		str += ", ts: " + ts;
		str += " }";
		return str;
	}
}
