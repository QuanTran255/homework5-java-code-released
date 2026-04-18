package test;

import static org.junit.Assert.*;

import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import dsl.*;
import ra.*;
import utils.Or;
import utils.Pair;
import utils.functions.Func2;

public class UTestRA {

	@Before
	public void setUp() {
		// nothing to do
	}

	@Test
	public void testThetaJoin() {
		System.out.println("***** Test ThetaJoin *****");
	
		BiPredicate<Integer,Double> theta = (i, x) -> Math.floor(x) == i;
		Query<Or<Integer,Double>,Pair<Integer,Double>> q = ThetaJoin.from(theta);
		SLastCount<Pair<Integer,Double>> sink = S.lastCount();
		
		int n = 1000;
		int m = 10;
		q.start(sink);
		assertEquals(0, sink.count);
		for (int i=0; i<n; i++) {
			for (int j=0; j<m; j++) {
				double x = i + (j / (double) m);
				q.next(Or.inr(x), sink);
				assertEquals(i * (long) m, sink.count);
			}
			q.next(Or.inl(i), sink);
			System.out.println(sink.last);
			assertEquals((i + 1) * (long) m, sink.count);
		}
		q.end(sink);
		assertEquals(n * (long) m, sink.count);
	}

	@Test
	public void testEquiJoin() {
		System.out.println("***** Test EquiJoin *****");
	
		Function<Integer,Integer> f = x -> x;
		Function<Double,Integer> g = x -> (int) Math.floor(x);
		Query<Or<Integer,Double>,Pair<Integer,Double>> q = EquiJoin.from(f, g);
		SLastCount<Pair<Integer,Double>> sink = S.lastCount();
		
		int n = 1000;
		int m = 10;
		q.start(sink);
		assertEquals(0, sink.count);
		for (int i=0; i<n; i++) {
			for (int j=0; j<m; j++) {
				double x = i + (j / (double) m);
				q.next(Or.inr(x), sink);
				assertEquals(i * (long) m, sink.count);
			}
			q.next(Or.inl(i), sink);
			System.out.println(sink.last);
			assertEquals((i + 1) * (long) m, sink.count);
		}
		q.end(sink);
		assertEquals(n * (long) m, sink.count);
	}


	@Test
	public void testGroupBy() {
		System.out.println("***** Test GroupBy *****");
	
		Func2<Double,Integer,Double> op = (x, i) -> x + i;
		Query<Pair<String,Integer>,Pair<String,Double>> q = GroupBy.from(0.0, op);
		SLastCount<Pair<String,Double>> sink = S.lastCount();
		
		List<Pair<String,Integer>> input = List.of(
			Pair.from("C", 3), Pair.from("A", 1), Pair.from("B", 2),
			Pair.from("B", 30), Pair.from("A", 20), Pair.from("C", 10),
			Pair.from("A", 300), Pair.from("B", 200), Pair.from("C", 100),
			Pair.from("B", 3000), Pair.from("C", 2000), Pair.from("A", 1000)
		);

		q.start(sink);
		assertEquals(0, sink.count);
		for (Pair<String,Integer> p: input) {
			q.next(p, sink);
			assertEquals(0, sink.count);
		}
		q.end(sink);
		assertEquals(3, sink.count);
		assertEquals("B", sink.last.getLeft());
		assertEquals(Double.valueOf(3232.0), sink.last.getRight());
	}	

}
