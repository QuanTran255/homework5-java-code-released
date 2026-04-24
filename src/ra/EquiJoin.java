package ra;

import java.util.function.Function;

import dsl.Query;
import dsl.Sink;
import utils.Or;
import utils.Pair;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

// A streaming implementation of the equi-join operator.
//
// We view the input as consisting of two channels:
// one with items of type A and one with items of type B.
// The output should contain all pairs (a, b) of input items,
// where a \in A is from the left channel, b \in B is from the
// right channel, and the equality predicate f(a) = g(b) holds.

public class EquiJoin<A,B,T> implements Query<Or<A,B>,Pair<A,B>> {

	Map<T,List<A>> lefts;
	Map<T,List<B>> rights;
	Function<A,T> f;
	Function<B,T> g;

	private EquiJoin(Function<A,T> f, Function<B,T> g) {
		this.lefts = new HashMap<>();
		this.rights = new HashMap<>();
		this.f = f;
		this.g = g;
	}

	public static <A,B,T> EquiJoin<A,B,T> from(Function<A,T> f, Function<B,T> g) {
		return new EquiJoin<>(f, g);
	}

	@Override
	public void start(Sink<Pair<A,B>> sink) {
		this.lefts.clear();
		this.rights.clear();
	}

	@Override
	public void next(Or<A,B> item, Sink<Pair<A,B>> sink) {
		if (item.isLeft()) {
			A a = item.getLeft();
			T key = f.apply(a);
			this.lefts.putIfAbsent(key, new ArrayList<>());
			this.lefts.get(key).add(a);
			if (this.rights.containsKey(key)) {
				for (B b : this.rights.get(key)) {
					sink.next(Pair.from(a, b));
				}
			}
		} else {
			B b = item.getRight();
			T key = g.apply(b);
			this.rights.putIfAbsent(key, new ArrayList<>());
			this.rights.get(key).add(b);
			if (this.lefts.containsKey(key)) {
				for (A a : this.lefts.get(key)) {
					sink.next(Pair.from(a, b));
				}
			}
		}
	}

	@Override
	public void end(Sink<Pair<A,B>> sink) {
		sink.end();
	}
	
}
