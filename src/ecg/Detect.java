package ecg;

import dsl.Query;
import dsl.Sink;

// The detection algorithm (decision rule) that we described in class
// (or your own slight variant of it).
//
// (1) Determine the threshold using the class TrainModel.
//
// (2) When l[n] exceeds the threhold, search for peak (max x[n] or raw[n])
//     in the next 40 samples.
//
// (3) No peak should be detected for 72 samples after the last peak.
//
// OUTPUT: The timestamp of each peak.

public class Detect implements Query<VTL,Long> {

	// Choose this to be two times the average length
	// over the entire signal.
	private static final double THRESHOLD = 0.0; // TODO

	// TODO

	public Detect() {
		// TODO
	}

	@Override
	public void start(Sink<Long> sink) {
		// TODO
	}

	@Override
	public void next(VTL item, Sink<Long> sink) {
		// TODO
	}

	@Override
	public void end(Sink<Long> sink) {
		// TODO
	}
	
}
