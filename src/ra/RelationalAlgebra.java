package ra;

import java.text.DecimalFormat;
import java.util.function.BiPredicate;
import java.util.function.Function;

import dsl.*;
import utils.Or;
import utils.Pair;

public class RelationalAlgebra {

	public static void main(String[] args) {
		System.out.println("*****************************************");
		System.out.println("***** ToyDSL and Relational Algebra *****");
		System.out.println("*****************************************");
		System.out.println();

		int n = 1000;
		int m = 1000;

		System.out.println("***** Equi-Join *****");
		{
			Function<Integer,Integer> f = x -> x;
			Function<Double,Integer> g = x -> (int) Math.floor(x);
			Query<Or<Integer,Double>,Pair<Integer,Double>> q = EquiJoin.from(f, g);
			execute(n, m, q, S.lastCount());
		}
		System.out.println();

		System.out.println("***** Theta Join *****");
		{
			BiPredicate<Integer,Double> theta = (i, x) -> Math.floor(x) == i;
			Query<Or<Integer,Double>,Pair<Integer,Double>> q = ThetaJoin.from(theta);
			execute(n, m, q, S.lastCount());
		}
		System.out.println();
	}

	private static long execute(
		int n, int m,
		Query<Or<Integer,Double>,Pair<Integer,Double>> q,
		Sink<Pair<Integer,Double>> sink
	) {
		long nTotal = 0;
		long start = System.nanoTime();

		q.start(sink);
		for (int i=0; i<n; i++) {
			for (int j=0; j<m; j++) {
				double x = i + (j / (double) m);
				q.next(Or.inr(x), sink);
				nTotal += 1;
			}
			q.next(Or.inl(i), sink);
			nTotal += 1;
		}
		q.end(sink);		
		
		long end = System.nanoTime();
		
		DecimalFormat formatter = new DecimalFormat("#,###");
		long timeNano = end - start;
		long timeMsec = timeNano / 1_000_000;
		System.out.println("duration = " + formatter.format(timeMsec) + " msec");
		long throughput = (nTotal * 1000L * 1000 * 1000) / timeNano;
		System.out.println("throughput = " + formatter.format(throughput) + " tuples/sec");

		return throughput;
	}

}
