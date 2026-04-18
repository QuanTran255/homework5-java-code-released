package dsl;

// Emit a value in the beginning (n times) and then echo
// the input stream.

public class Emit<A> implements Query<A,A> {

	private final int n;
	private final A value;

	public Emit(int n, A value) {
		if (n < 0) {
			throw new IllegalArgumentException("Emit: n must be >= 0");
		}
		this.n = n;
		this.value = value;
	}

	@Override
	public void start(Sink<A> sink) {
		for (int i=0; i<n; i++) {
			sink.next(value);
		}
	}

	@Override
	public void next(A item, Sink<A> sink) {
		sink.next(item);
	}

	@Override
	public void end(Sink<A> sink) {
		sink.end();
	}
	
}
