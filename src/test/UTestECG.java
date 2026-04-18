package test;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import dsl.*;
import ecg.*;

public class UTestECG {

	@Before
	public void setUp() {
		// nothing to do
	}

	@Test
	public void testIntervals() {
		System.out.println("***** Test RR Intervals *****");
	
		Query<Integer,Double> q = HeartRate.qIntervals();
		SLastCount<Double> sink = S.lastCount();
		
		Q.execute(Data.ecgStream("100.csv"), q, sink);

		assertEquals(16, sink.count);
		double threshold = sink.last;
		assert(300.0 <= threshold && threshold <= 1500.0);
	}

	@Test
	public void testHeartRate() {
		System.out.println("***** Test Average Heart Rate *****");
	
		Query<Integer,Double> q = HeartRate.qHeartRateAvg();
		SLastCount<Double> sink = S.lastCount();
		
		Q.execute(Data.ecgStream("100.csv"), q, sink);

		assertEquals(1, sink.count);
		double threshold = sink.last;
		assert(50.0 <= threshold && threshold <= 100.0);
	}

	@Test
	public void testSDNN() {
		System.out.println("***** Test SDNN *****");
	
		Query<Integer,Double> q = HeartRate.qSDNN();
		SLastCount<Double> sink = S.lastCount();
		
		Q.execute(Data.ecgStream("100.csv"), q, sink);

		assertEquals(1, sink.count);
		double threshold = sink.last;
		assert(10.0 <= threshold && threshold <= 200.0);
	}

	@Test
	public void testRMSSD() {
		System.out.println("***** Test RMSSD *****");
	
		Query<Integer,Double> q = HeartRate.qRMSSD();
		SLastCount<Double> sink = S.lastCount();
		
		Q.execute(Data.ecgStream("100.csv"), q, sink);

		assertEquals(1, sink.count);
		double threshold = sink.last;
		assert(10.0 <= threshold && threshold <= 200.0);
	}

	@Test
	public void testPNN50() {
		System.out.println("***** Test pNN50 *****");
	
		Query<Integer,Double> q = HeartRate.qPNN50();
		SLastCount<Double> sink = S.lastCount();
		
		Q.execute(Data.ecgStream("100.csv"), q, sink);

		assertEquals(1, sink.count);
		double threshold = sink.last;
		assert(1.0 <= threshold && threshold <= 50.0);
	}

	@Test
	public void testThreshold() {
		System.out.println("***** Test Threshold *****");
	
		Query<Integer,Double> q = TrainModel.qLengthAvg();
		SLastCount<Double> sink = S.lastCount();
		
		Q.execute(Data.ecgStream("100.csv"), q, sink);

		assertEquals(1, sink.count);
		double threshold = sink.last;
		assert(10 <= threshold && threshold <= 1000);
	}

	@Test
	public void testPeakDetection01() {
		System.out.println("***** Test Peak Detection (1) *****");
	
		Query<Integer,Long> q = PeakDetection.qPeaks();
		SLastCount<Long> sink = S.lastCount();
		
		Q.execute(Data.ecgStream("100-samples-200.csv"), q, sink);

		assertEquals(1, sink.count);
		long tsPeak1 = sink.last.longValue();
		assertEquals(77, tsPeak1);
	}

	@Test
	public void testPeakDetection02() {
		System.out.println("***** Test Peak Detection (2) *****");
	
		Query<Integer,Long> q = PeakDetection.qPeaks();
		SLastCount<Long> sink = S.lastCount();
		
		Q.execute(Data.ecgStream("100-samples-500.csv"), q, sink);

		assertEquals(2, sink.count);
		long tsPeak2 = sink.last.longValue();
		assertEquals(370, tsPeak2);
	}

	@Test
	public void testPeakDetection03() {
		System.out.println("***** Test Peak Detection (3) *****");
	
		Query<Integer,Long> q = PeakDetection.qPeaks();
		SLastCount<Long> sink = S.lastCount();
		
		Q.execute(Data.ecgStream("100-samples-800.csv"), q, sink);

		assertEquals(3, sink.count);
		long tsPeak3 = sink.last.longValue();
		assertEquals(663, tsPeak3);
	}

	@Test
	public void testPeakDetection04() {
		System.out.println("***** Test Peak Detection (4) *****");
	
		Query<Integer,Long> q = PeakDetection.qPeaks();
		SLastCount<Long> sink = S.lastCount();
		
		Q.execute(Data.ecgStream("100-samples-1000.csv"), q, sink);

		assertEquals(4, sink.count);
		long tsPeak3 = sink.last.longValue();
		assertEquals(947, tsPeak3);
	}

	@Test
	public void testPeakDetection17() {
		System.out.println("***** Test Peak Detection (17) *****");
	
		Query<Integer,Long> q = PeakDetection.qPeaks();
		SLastCount<Long> sink = S.lastCount();
		
		Q.execute(Data.ecgStream("100.csv"), q, sink);

		assertEquals(17, sink.count);
		long tsPeak3 = sink.last.longValue();
		assertEquals(4765, tsPeak3);
	}

}
