package dsl;

import java.util.function.Function;

// Apply a function elementwise.

public class Map<A,B> implements Query<A,B> {

	private final Function<A,B> op;

	public Map(Function<A,B> op) {
		this.op = op;
	}

	@Override
	public void start(Sink<B> sink) {
		// nothing to do
	}

	@Override
	public void next(A item, Sink<B> sink) {
		sink.next(op.apply(item));
	}

	@Override
	public void end(Sink<B> sink) {
		sink.end();
	}
	
}
