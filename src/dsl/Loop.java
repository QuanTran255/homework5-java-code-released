package dsl;

import java.util.ArrayDeque;

import utils.Or;

// Feedback composition.

public class Loop<A,B> implements Query<A,B> {

	private final Query<Or<A,B>,B> q;
	private ArrayDeque<B> buffer;
	private boolean ended;

	public Loop(Query<Or<A,B>,B> q) {
		this.q = q;
		this.buffer = new ArrayDeque<>();
	}

	private Sink<B> intermediate(Sink<B> sink) {
		return new Sink<B>() {
			@Override
			public void next(B item) {
				buffer.add(item);
				sink.next(item);
			}
			@Override
			public void end() {
				ended = true;
			}
		};
	}

	private void drain(Sink<B> sink, Sink<B> isink) {
		while (!buffer.isEmpty()) {
			B b = buffer.remove();
			q.next(Or.inr(b), isink);	
		}
		if (ended) {
			sink.end();
		}
	}

	@Override
	public void start(Sink<B> sink) {
		this.buffer = new ArrayDeque<>();
		ended = false;

		Sink<B> isink = intermediate(sink);
		q.start(isink);
		drain(sink, isink);
	}

	@Override
	public void next(A item, Sink<B> sink) {
		if (!ended) {
			Sink<B> isink = intermediate(sink);
			q.next(Or.inl(item), isink);
			drain(sink, isink);
		}
	}

	@Override
	public void end(Sink<B> sink) {
		if (!ended) {
			Sink<B> isink = intermediate(sink);
			q.end(isink);
			drain(sink, isink);
		}
	}
	
}
