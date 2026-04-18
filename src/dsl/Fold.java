package dsl;

import utils.functions.Func2;

// Aggregation (one output item when the stream ends).

public class Fold<A, B> implements Query<A, B> {

	private final B init;
	private final Func2<B,A,B> op;
	private B agg; // current aggregate

	public Fold(B init, Func2<B,A,B> op) {
		this.init = init;
		this.op = op;
	}

	@Override
	public void start(Sink<B> sink) {
		this.agg = init;
	}

	@Override
	public void next(A item, Sink<B> sink) {
		agg = op.apply(agg, item);
	}

	@Override
	public void end(Sink<B> sink) {
		sink.next(agg);
		sink.end();
	}
	
}
