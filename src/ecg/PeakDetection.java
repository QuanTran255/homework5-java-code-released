package ecg;

import dsl.S;
import dsl.Q;
import dsl.Query;

public class PeakDetection {

	// The curve length transformation:
	//
	// adjust: x[n] = raw[n] - 1024
	// smooth: y[n] = (x[n-2] + x[n-1] + x[n] + x[n+1] + x[n+2]) / 5
	// deriv: d[n] = (y[n+1] - y[n-1]) / 2
	// length: l[n] = t(d[n-w]) + ... + t(d[n+w]), where
	//         w = 20 (samples) and t(d) = sqrt(1.0 + d * d)

	public static Query<Integer,Double> qLength() {
		// adjust >> smooth >> deriv >> length

		Query<Integer,Double> adjust = Q.map(x -> (double)(x - 1024));

		Query<Double,Double> smoothSum = Q.sWindowNaive(5, 0.0, (acc, a) -> acc + a);
		Query<Double,Double> smooth = Q.pipeline(smoothSum, Q.map(s -> s / 5.0));

		Query<Double,Double> deriv = Q.sWindow3((y0, y1, y2) -> (y2 - y0) / 2.0);

		Query<Double,Double> tmap = Q.map(d -> Math.sqrt(1.0 + d * d));

		Query<Double,Double> length = Q.sWindowNaive(41, 0.0, (acc, a) -> acc + a);

		return Q.pipeline(adjust, smooth, deriv, tmap, length);
	}

	// In order to detect peaks we need both the raw (or adjusted)
	// signal and the signal given by the curve length transformation.
	// Use the datatype VTL and implement the class Detect.

	public static Query<Integer,Long> qPeaks() {

		// qVT: Query<Integer,VT> pairs each input value with a sample timestamp
		Query<Integer,VT> qVT = Q.parallel(Q.id(), Q.scan(0L, (ts, a) -> ts + 1L), (v, ts) -> new VT(v, ts));

		// qVTL: Query<Integer,VTL> pairs VT with the length transform value
		Query<Integer,VTL> qVTL = Q.parallel(qVT, qLength(), (vt, l) -> vt.extendl(l));

		// Create detector: default no-arg uses static THRESHOLD or can be overloaded
		Detect detect = new Detect();

		return Q.pipeline(qVTL, detect);
	}

	public static void main(String[] args) {
		System.out.println("****************************************");
		System.out.println("***** Algorithm for Peak Detection *****");
		System.out.println("****************************************");
		System.out.println();

		Q.execute(Data.ecgStream("100.csv"), qPeaks(), S.printer());
	}

}
