package dsl;

import utils.functions.Func2;

// Running aggregation (one output item per input item).

public class Scan<A, B> implements Query<A, B> {

	private final B init;
	private final Func2<B,A,B> op;
	private B agg; // current aggregate

	public Scan(B init, Func2<B,A,B> op) {
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
		sink.next(agg);
	}

	@Override
	public void end(Sink<B> sink) {
		sink.end();
	}
	
}
