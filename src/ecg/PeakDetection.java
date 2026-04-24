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

		Query<Double,Double> dLength = Q.map(d -> Math.sqrt(1.0 + d * d));

		Query<Double,Double> length = Q.sWindowNaive(41, 0.0, (acc, a) -> acc + a);

		return Q.pipeline(adjust, smooth, deriv, dLength, length);
	}

	// In order to detect peaks we need both the raw (or adjusted)
	// signal and the signal given by the curve length transformation.
	// Use the datatype VTL and implement the class Detect.

	public static Query<Integer,Long> qPeaks() {
		// qLength() startup latency: smooth(5-1=4) + deriv(3-1=2) + length(41-1=40) - 1 = 45 samples
		// Both sides of Q.parallel must have the same latency
		int LATENCY = 45;

		// Delay raw values by LATENCY samples so both sides of parallel start together
		Query<Integer,Integer> qDelayedV = Q.sWindowNaive(LATENCY + 1, 0, (acc, a) -> a);

		// Pair current raw value with its length, then stamp with the correct timestamp
		// scan uses the previous VTL's ts as the counter
		Query<Integer,VTL> qVTL = Q.pipeline(
			Q.parallel(qDelayedV, qLength(), (v, l) -> new VTL(v, 0L, l)),
			Q.scan(new VTL(0, LATENCY - 1L, 0.0), (prev, vtl) -> new VTL(vtl.v, prev.ts + 1L, vtl.l))
		);

		return Q.pipeline(qVTL, new Detect());
	}

	public static void main(String[] args) {
		System.out.println("****************************************");
		System.out.println("***** Algorithm for Peak Detection *****");
		System.out.println("****************************************");
		System.out.println();

		Q.execute(Data.ecgStream("100.csv"), qPeaks(), S.printer());
	}

}
