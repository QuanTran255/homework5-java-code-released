package dsl;

import java.util.function.BinaryOperator;

// Efficient algorithm for aggregation over a sliding window.
// It assumes that there is a 'remove' operation for updating
// the aggregate when an element is evicted from the window.

public class SWindowInv<A> implements Query<A,A> {

	private final A init;
	private final BinaryOperator<A> insert;
	private final BinaryOperator<A> remove;
	private final int wndSize; // window size
	private final A[] buffer;
	private A agg; // current aggregate
	private int indexOldest; // index to oldest element
	private int nElements; // number of elements in buffer

	public SWindowInv
	(int wndSize, A init, BinaryOperator<A> insert, BinaryOperator<A> remove)
	{
		if (wndSize < 1) {
			throw new IllegalArgumentException("window size should be >= 1");
		}
		this.init = init;
		this.insert = insert;
		this.remove = remove;
		this.wndSize = wndSize;
		this.buffer = (A[]) new Object[wndSize];
		this.agg = init;
		this.indexOldest = 0;
		this.nElements = 0;
	}

	@Override
	public void start(Sink<A> sink) {
		this.agg = init;
		this.indexOldest = 0;
		this.nElements = 0;
	}

	@Override
	public void next(A item, Sink<A> sink) {
		if (nElements == wndSize) {
			agg = remove.apply(agg, buffer[indexOldest]);
			buffer[indexOldest] = item;
			indexOldest = (indexOldest + 1) % wndSize;
			agg = insert.apply(agg, item);
		} else { // nElements < wndSize
			buffer[nElements] = item;
			nElements += 1;
			agg = insert.apply(agg, item);
		}
		if (nElements == wndSize) {
			sink.next(agg);
		}
	}

	@Override
	public void end(Sink<A> sink) {
		sink.end();
	}
	
}
