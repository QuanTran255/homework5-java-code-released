package dsl;

import utils.functions.Func3;

// Sliding window of size 3.

public class SWindow3<A,B> implements Query<A,B> {

	private final Func3<A,A,A,B> op;
	private A previous0;
	private A previous1;
	private int nElements; // number of elements in buffer

	public SWindow3(Func3<A,A,A,B> op) {
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
		if (nElements >= 2) {
			agg = op.apply(previous0, previous1, item);
			nElements = 3;
			sink.next(agg);
		} else { // nElements == 0
			nElements += 1;
		}
		previous0 = previous1;
		previous1 = item;
	}

	@Override
	public void end(Sink<B> sink) {
		sink.end();
	}
	
}
