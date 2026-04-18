package dsl;

import utils.functions.Func2;

// Sliding window of size 2.

public class SWindow2<A,B> implements Query<A,B> {

	private final Func2<A,A,B> op;
	private A previous;
	private int nElements; // number of elements in buffer

	public SWindow2(Func2<A,A,B> op) {
		this.op = op;
		this.nElements = 0;
	}

	@Override
	public void start(Sink<B> sink) {
		this.nElements = 0;
	}

	@Override
	public void next(A item, Sink<B> sink) {
		B agg;
		if (nElements >= 1) {
			agg = op.apply(previous, item);
			nElements = 2;
			sink.next(agg);
		} else { // nElements == 0
			nElements += 1;
		}
		previous = item;
	}

	@Override
	public void end(Sink<B> sink) {
		sink.end();
	}
	
}
