package ra;

import java.util.function.BiPredicate;

import dsl.Query;
import dsl.Sink;
import utils.Or;
import utils.Pair;
import java.util.List;
import java.util.ArrayList;

// A streaming implementation of the theta join operator.
//
// We view the input as consisting of two channels:
// one with items of type A and one with items of type B.
// The output should contain all pairs (a, b) of input items,
// where a \in A is from the left channel, b \in B is from the
// right channel, and the pair (a, b) satisfies a predicate theta.

public class ThetaJoin<A,B> implements Query<Or<A,B>,Pair<A,B>> {

	List<A> lefts;
	List<B> rights;
	BiPredicate<A,B> theta;

	private ThetaJoin(BiPredicate<A,B> theta) {
		this.lefts = new ArrayList<>();
		this.rights = new ArrayList<>();
		this.theta = theta;
	}

	public static <A,B> ThetaJoin<A,B> from(BiPredicate<A,B> theta) {
		return new ThetaJoin<>(theta);
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
			this.lefts.add(a);
			for (B b : this.rights) {
				if (theta.test(a, b)) {
					sink.next(Pair.from(a, b));
				}
			}
		} else {
			B b = item.getRight();
			this.rights.add(b);
			for (A a : this.lefts) {
				if (theta.test(a, b)) {
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
