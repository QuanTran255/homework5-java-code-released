package ra;

import dsl.Query;
import dsl.Sink;
import utils.Pair;
import utils.functions.Func2;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

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

	private final B init;
	private final Func2<B,A,B> op;
	private Map<K,B> map;
	private List<K> order;

	private GroupBy(B init, Func2<B,A,B> op) {
		this.init = init;
		this.op = op;
		this.map = new HashMap<>();
		this.order = new ArrayList<>();
	}

	public static <K,A,B> GroupBy<K,A,B> from(B init, Func2<B,A,B> op) {
		return new GroupBy<>(init, op);
	}

	@Override
	public void start(Sink<Pair<K,B>> sink) {
		map.clear();
		order.clear();
	}

	@Override
	public void next(Pair<K,A> item, Sink<Pair<K,B>> sink) {
		K key = item.getLeft();
		A value = item.getRight();
		if (!map.containsKey(key)) {
			B aggregated = op.apply(init, value);
			map.put(key, aggregated);
			order.add(key);
		} else {
			B current = map.get(key);
			B aggregated = op.apply(current, value);
			map.put(key, aggregated);
		}
	}

	@Override
	public void end(Sink<Pair<K,B>> sink) {
		for (K key : order) {
			B value = map.get(key);
			sink.next(Pair.from(key, value));
		}
		sink.end();
	}
	
}
