package dsl;

import java.util.ArrayDeque;

import utils.functions.Func2;

// A variant of parallel composition, which is similar to 'zip'.

public class Parallel<A, B, C, D> implements Query<A, D> {

	private final Query<A,B> q1;
	private final Query<A,C> q2;
	private final Func2<B,C,D> op;
	private ArrayDeque<B> buffer1;
	private boolean ended1;
	private ArrayDeque<C> buffer2;
	private boolean ended2;

	public Parallel(Query<A,B> q1, Query<A,C> q2, Func2<B,C,D> op) {
		this.q1 = q1;
		this.q2 = q2;
		this.op = op;
		this.buffer1 = new ArrayDeque<>();
		this.buffer2 = new ArrayDeque<>();
	}

	private void process1(B b, Sink<D> sink) {
		if (buffer2.isEmpty()) {
			buffer1.add(b);
		} else {
			assert buffer1.isEmpty();
			C c = buffer2.remove();
			sink.next(op.apply(b, c));
		}
	}

	private Sink<B> left(Sink<D> sink) {
		return new Sink<B>() {
			@Override
			public void next(B item) {
				process1(item, sink);
			}
			@Override
			public void end() {
				ended1 = true;
				if (ended2) {
					//System.out.println("buffer1.size() = " + buffer1.size());
					//System.out.println("buffer2.size() = " + buffer2.size());
					sink.end();
				}
			}
		};
	}

	private void process2(C c, Sink<D> sink) {
		if (buffer1.isEmpty()) {
			buffer2.add(c);
		} else {
			assert buffer2.isEmpty();
			B b = buffer1.remove();
			sink.next(op.apply(b, c));
		}
	}

	private Sink<C> right(Sink<D> sink) {
		return new Sink<C>() {
			@Override
			public void next(C item) {
				process2(item, sink);
			}
			@Override
			public void end() {
				ended2 = true;
				if (ended1) {
					//System.out.println("buffer1.size() = " + buffer1.size());
					//System.out.println("buffer2.size() = " + buffer2.size());
					sink.end();
				}
			}
		};
	}

	@Override
	public void start(Sink<D> sink) {
		q1.start(left(sink));
		q2.start(right(sink));
	}

	@Override
	public void next(A item, Sink<D> sink) {
		q1.next(item, left(sink));
		q2.next(item, right(sink));
	}

	@Override
	public void end(Sink<D> sink) {
		q1.end(left(sink));
		q2.end(right(sink));
	}
	
}
