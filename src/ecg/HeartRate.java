package ecg;

import dsl.S;
import dsl.Q;
import dsl.Query;

// This file is devoted to the analysis of the heart rate of the patient.
// It is assumed that PeakDetection.qPeaks() has already been implemented.

public class HeartRate {

	// RR interval length (in milliseconds)
	public static Query<Integer,Double> qIntervals() {
		Query<Integer,Long> peaks = PeakDetection.qPeaks();
		Query<Long, Double> w2 = Q.sWindow2((t1, t2) -> (t2 - t1) * 1000.0 / 360.0);
		return Q.pipeline(peaks, w2);

	}

	// Average heart rate (over entire signal) in bpm.
	public static Query<Integer,Double> qHeartRateAvg() {
		Query<Integer,Long> peaks = PeakDetection.qPeaks();
		Query<Long, Double> w2 = Q.sWindow2((t1, t2) -> (t2 - t1) * 1000.0 / 360.0);
		return Q.pipeline(peaks, w2, Q.foldAvg(), Q.map(interval -> 60000.0 / interval));
	}

	// Standard deviation of NN interval length (over the entire signal)
	// in milliseconds.
	public static Query<Integer,Double> qSDNN() {
		Query<Integer,Long> peaks = PeakDetection.qPeaks();
		Query<Long, Double> w2 = Q.sWindow2((t1, t2) -> (t2 - t1) * 1000.0 / 360.0);
		return Q.pipeline(peaks, w2, Q.foldStdev());
	}

	// RMSSD measure (over the entire signal) in milliseconds.
	public static Query<Integer,Double> qRMSSD() {
		Query<Integer,Long> peaks = PeakDetection.qPeaks();
		Query<Long, Double> w2 = Q.sWindow2((t1, t2) -> (t2 - t1) * 1000.0 / 360.0);
		Query<Double, Double> w2diff = Q.sWindow2((i1, i2) -> i2 - i1);
		Query<Double, Double> sq = Q.map(x -> x * x);
		return Q.pipeline(peaks, w2, w2diff, sq, Q.foldAvg(), Q.map(avg -> Math.sqrt(avg)));
	}

	// Proportion (in %) derived by dividing NN50 by the total number
	// of NN intervals (calculated over the entire signal).
	public static Query<Integer,Double> qPNN50() {
		Query<Integer,Long> peaks = PeakDetection.qPeaks();
		Query<Long, Double> w2 = Q.sWindow2((t1, t2) -> (t2 - t1) * 1000.0 / 360.0);
		Query<Double, Double> w2diff = Q.sWindow2((i1, i2) -> i2 - i1);
		Query<Double, Double> nn50flag = Q.map(d -> Math.abs(d) > 50.0 ? 1.0 : 0.0);
		return Q.pipeline(peaks, w2, w2diff, nn50flag, Q.foldAvg(), Q.map(p -> p * 100.0));
	}

	public static void main(String[] args) {
		System.out.println("****************************************");
		System.out.println("***** Algorithm for the Heart Rate *****");
		System.out.println("****************************************");
		System.out.println();

		System.out.println("***** Intervals *****");
		Q.execute(Data.ecgStream("100.csv"), qIntervals(), S.printer());
		System.out.println();

		System.out.println("***** Average heart rate *****");
		Q.execute(Data.ecgStream("100-all.csv"), qHeartRateAvg(), S.printer());
		System.out.println();

		System.out.println("***** HRV Measure: SDNN *****");
		Q.execute(Data.ecgStream("100-all.csv"), qSDNN(), S.printer());
		System.out.println();

		System.out.println("***** HRV Measure: RMSSD *****");
		Q.execute(Data.ecgStream("100-all.csv"), qRMSSD(), S.printer());
		System.out.println();

		System.out.println("***** HRV Measure: pNN50 *****");
		Q.execute(Data.ecgStream("100-all.csv"), qPNN50(), S.printer());
		System.out.println();
	}

}
