package ra;

import dsl.Query;
import dsl.Sink;
import utils.Pair;
import utils.functions.Func2;

// A streaming implementation of the "group by" (and aggregate) operator.
//
// The input consists of one channel that carries key-value pairs of the
// form (k, a) where k \in K and a \in A.
// For every key k, we perform a separate aggregation in the style of fold.
// When the input stream ends, we output all results (k, b), where k is
// a key and b is the aggregate for k.
//
// The keys in the output should be given in the order of their first occurrence
// in the input stream. That is, if k1 occurred earlier than k2 in the input
// stream, then the output (k1, b1) should be given before (k2, b2) in the
// output.

public class GroupBy<K,A,B> implements Query<Pair<K,A>,Pair<K,B>> {

	// TODO

	private GroupBy(B init, Func2<B,A,B> op) {
		// TODO
	}

	public static <K,A,B> GroupBy<K,A,B> from(B init, Func2<B,A,B> op) {
		return new GroupBy<>(init, op);
	}

	@Override
	public void start(Sink<Pair<K,B>> sink) {
		// TODO
	}

	@Override
	public void next(Pair<K,A> item, Sink<Pair<K,B>> sink) {
		// TODO
	}

	@Override
	public void end(Sink<Pair<K,B>> sink) {
		// TODO
	}
	
}
