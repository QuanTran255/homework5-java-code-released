package dsl;

import utils.functions.Func2;

// Naive algorithm for aggregation over a sliding window.

public class SWindowNv<A,B> implements Query<A,B> {

	private final B init;
	private final Func2<B,A,B> op;
	private final int wndSize; // window size
	private final A[] buffer;
	private int indexOldest; // index to oldest element
	private int nElements; // number of elements in buffer

	public SWindowNv(int wndSize, B init, Func2<B,A,B> op) {
		if (wndSize < 1) {
			throw new IllegalArgumentException("window size should be >= 1");
		}
		this.init = init;
		this.op = op;
		this.wndSize = wndSize;
		this.buffer = (A[]) new Object[wndSize];
		this.indexOldest = 0;
		this.nElements = 0;
	}

	@Override
	public void start(Sink<B> sink) {
		this.indexOldest = 0;
		this.nElements = 0;
	}

	@Override
	public void next(A item, Sink<B> sink) {
		if (nElements == wndSize) {
			buffer[indexOldest] = item;
			indexOldest = (indexOldest + 1) % wndSize;
		} else { // nElements < wndSize
			buffer[nElements] = item;
			nElements += 1;
		}
		if (nElements == wndSize) {
			B agg = init;
			int index = indexOldest;
			for (int i=0; i<nElements; i++) {
				agg = op.apply(agg, buffer[index]);
				index = (index + 1) % wndSize;
			}
			sink.next(agg);	
		}
	}

	@Override
	public void end(Sink<B> sink) {
		sink.end();
	}
	
}
