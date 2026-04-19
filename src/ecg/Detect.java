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
	private static final double THRESHOLD = 127.65300978749397;

	private enum State { IDLE, SEARCH, PAUSE }

	// Sampling rate (samples per second)
	private static final int SAMPLING_RATE = 360;
	private static final int DEFAULT_SEARCH_MS = 110;
	private static final int DEFAULT_REFRACTORY_MS = 200;
	private static final int DEFAULT_SEARCH_SAMPLES = (DEFAULT_SEARCH_MS * SAMPLING_RATE + 999) / 1000;
	private static final int DEFAULT_REFRACTORY_SAMPLES = (DEFAULT_REFRACTORY_MS * SAMPLING_RATE + 999) / 1000;

	private final double threshold;
	private final int searchWindowSamples; // search interval in samples
	private final int refractorySamples; // refractory period in samples

	// mutable state
	private State state;
	private int searchRemaining;
	private int refracRemaining;
	private int currentMaxV;
	private long currentMaxTs;

	public Detect() {
		this.threshold = THRESHOLD;
		this.searchWindowSamples = DEFAULT_SEARCH_SAMPLES;
		this.refractorySamples = DEFAULT_REFRACTORY_SAMPLES;
		this.state = State.IDLE;
		this.searchRemaining = 0;
		this.refracRemaining = 0;
		this.currentMaxV = Integer.MIN_VALUE;
		this.currentMaxTs = -1L;
	}

	@Override
	public void start(Sink<Long> sink) {
		this.state = State.IDLE;
		this.searchRemaining = 0;
		this.refracRemaining = 0;
		this.currentMaxV = Integer.MIN_VALUE;
		this.currentMaxTs = -1L;
	}

	@Override
	public void next(VTL item, Sink<Long> sink) {
		switch (state) {
		case IDLE:
			if (item.l > threshold) {
				// start searching and include current sample as first sample
				state = State.SEARCH;
				currentMaxV = item.v;
				currentMaxTs = item.ts;
				searchRemaining = Math.max(0, searchWindowSamples - 1);
			}
			break;
		case SEARCH:
			if (item.v > currentMaxV) {
				currentMaxV = item.v;
				currentMaxTs = item.ts;
			}
			if (searchRemaining > 0) {
				searchRemaining -= 1;
			}
			if (searchRemaining == 0) {
				// emit detected peak timestamp
				sink.next(currentMaxTs);
				state = State.PAUSE;
				refracRemaining = refractorySamples;
			}
			break;
		case PAUSE:
			if (refracRemaining > 0) {
				refracRemaining -= 1;
			}
			if (refracRemaining == 0) {
				state = State.IDLE;
			}
			break;
		}
	}

	@Override
	public void end(Sink<Long> sink) {
		// if stream ends while searching, emit the best candidate
		if (state == State.SEARCH && currentMaxTs >= 0) {
			sink.next(currentMaxTs);
		}
		sink.end();
	}
	
}
