package test;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import dsl.*;
import compress.*;
import ecg.Data;

public class UTestCompress {

	@Before
	public void setUp() {
		// nothing to do
	}

	@Test
	public void testCompress1() {
		System.out.println("***** Test Compress (1) *****");

		Integer[] arr = new Integer[] {
			100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
			0, 255, 0, 0, 255, 0, 0, 255, 0, 255
		};
		assert arr.length % Compress.BLOCK_SIZE == 0;
	
		Query<Integer,Integer> enc = Q.pipeline(
			Compress.delta(), Compress.zigzag()
		);
		Query<Integer,Integer> check = Q.map(x -> {
			if (x < 0 || x > 510) {
				throw new IllegalArgumentException();
			}
			return x;
		});
		Query<Integer,Integer> dec = Q.pipeline(
			Compress.zigzagInv(), Compress.deltaInv()
		);
		Query<Integer,Integer> q = Q.pipeline(enc, check, dec);
		SLastCount<Integer> sink = S.lastCount();
		
		int n = arr.length;
		q.start(sink);
		assertEquals(0, sink.count);
		for (int i = 0; i < n; i++) {
			assertEquals(i, sink.count);
			q.next(arr[i], sink);
			assertEquals(arr[i], sink.last);
		}
		q.end(sink);
		assertEquals(n, sink.count);
	}

	@Test
	public void testCompress2() {
		System.out.println("***** Test Compress (2) *****");

		Integer[] arr = new Integer[] {
			100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
			108, 107, 106, 105, 104, 103, 102, 101, 100, 99,
			100, 110, 120, 130, 140, 150, 160, 170, 180, 190
		};
		assert arr.length % Compress.BLOCK_SIZE == 0;
	
		Query<Integer,Integer> q = Compress.compress();
		SLastCount<Integer> sink = S.lastCount();
		
		int n = arr.length;
		q.start(sink);
		for (int i = 0; i < n; i++) {
			q.next(arr[i], sink);
		}
		q.end(sink);

		// compress to at most 20 bytes
		assert(sink.count <= 20);
	}

	@Test
	public void testCompress3() {
		System.out.println("***** Test Compress (3) *****");

		Integer[] arr = new Integer[] {
			100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
			108, 107, 106, 105, 104, 103, 102, 101, 100, 99,
			100, 110, 120, 130, 140, 150, 160, 170, 180, 190
		};
		assert arr.length % Compress.BLOCK_SIZE == 0;
	
		Query<Integer,Integer> enc = Compress.compress();
		Query<Integer,Integer> check = Q.map(x -> {
			if (x < 0 || x > 255) {
				throw new IllegalArgumentException();
			}
			return x;
		});
		Query<Integer,Integer> dec = Compress.decompress();
		Query<Integer,Integer> q = Q.pipeline(enc, check, dec);
		SCollector<Integer> sink = S.collector();
		
		int n = arr.length;
		q.start(sink);
		for (int i = 0; i < n; i++) {
			if (i % Compress.BLOCK_SIZE == 0) {
				assertEquals(i, sink.list.size());
			}
			q.next(arr[i], sink);
		}
		q.end(sink);
		assertEquals(n, sink.list.size());

		for (int i = 0; i < n; i++) {
			assertEquals(arr[i], sink.list.get(i));
		}
	}

	@Test
	public void testCompress4() {
		System.out.println("***** Test Compress (4) *****");

		Integer[] arr = new Integer[] {
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			12, 14, 16, 18, 20, 22, 24, 26, 28, 30
		};
		assert arr.length % Compress.BLOCK_SIZE == 0;
	
		Query<Integer,Integer> q = Compress.compress();
		SLastCount<Integer> sink = S.lastCount();
		
		int n = arr.length;
		q.start(sink);
		for (int i = 0; i < n; i++) {
			q.next(arr[i], sink);
		}
		q.end(sink);

		// compress to at most 10 bytes
		assert(sink.count <= 10);
	}

	@Test
	public void testCompress5() {
		System.out.println("***** Test Compress (5) *****");

		Integer[] arr = new Integer[] {
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			12, 14, 16, 18, 20, 22, 24, 26, 28, 30
		};
		assert arr.length % Compress.BLOCK_SIZE == 0;
	
		Query<Integer,Integer> enc = Compress.compress();
		Query<Integer,Integer> check = Q.map(x -> {
			if (x < 0 || x > 255) {
				throw new IllegalArgumentException();
			}
			return x;
		});
		Query<Integer,Integer> dec = Compress.decompress();
		Query<Integer,Integer> q = Q.pipeline(enc, check, dec);
		SCollector<Integer> sink = S.collector();
		
		int n = arr.length;
		q.start(sink);
		for (int i = 0; i < n; i++) {
			if (i % Compress.BLOCK_SIZE == 0) {
				assertEquals(i, sink.list.size());
			}
			q.next(arr[i], sink);
		}
		q.end(sink);
		assertEquals(n, sink.list.size());

		for (int i = 0; i < n; i++) {
			assertEquals(arr[i], sink.list.get(i));
		}
	}

	@Test
	public void testCompress6() {
		System.out.println("***** Test Compress (6) *****");

		Iterator<Integer> it = Data.ecgStream("100.csv");
		// from range [0,2048) to [0,256)
		Query<Integer,Integer> qIn = Q.map(x -> x / 8);
		SCollector<Integer> inSink = S.collector();
		Q.execute(it, qIn, inSink);
		List<Integer> arr = inSink.list;
		assert arr.size() % Compress.BLOCK_SIZE == 0;

		Query<Integer,Integer> q = Compress.compress();
		SLastCount<Integer> sink = S.lastCount();
		
		int n = arr.size();
		q.start(sink);
		for (int i = 0; i < n; i++) {
			q.next(arr.get(i), sink);
		}
		q.end(sink);

		assert(arr.size() == 5000);
		// compress to at most 1800 bytes
		assert(sink.count <= 1800);
	}

	@Test
	public void testCompress7() {
		System.out.println("***** Test Compress (7) *****");

		Iterator<Integer> it = Data.ecgStream("100.csv");
		// from range [0,2048) to [0,256)
		Query<Integer,Integer> qIn = Q.map(x -> x / 8);
		SCollector<Integer> inSink = S.collector();
		Q.execute(it, qIn, inSink);
		List<Integer> arr = inSink.list;
		assert arr.size() % Compress.BLOCK_SIZE == 0;

		Query<Integer,Integer> enc = Compress.compress();
		Query<Integer,Integer> check = Q.map(x -> {
			if (x < 0 || x > 255) {
				throw new IllegalArgumentException();
			}
			return x;
		});
		Query<Integer,Integer> dec = Compress.decompress();
		Query<Integer,Integer> q = Q.pipeline(enc, check, dec);
		SCollector<Integer> sink = S.collector();
		
		int n = arr.size();
		q.start(sink);
		for (int i = 0; i < n; i++) {
			if (i % Compress.BLOCK_SIZE == 0) {
				assertEquals(i, sink.list.size());
			}
			q.next(arr.get(i), sink);
		}
		q.end(sink);
		assertEquals(n, sink.list.size());

		for (int i = 0; i < n; i++) {
			assertEquals(arr.get(i), sink.list.get(i));
		}
	}

}
