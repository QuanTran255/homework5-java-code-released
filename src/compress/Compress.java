package compress;

import java.util.Iterator;

import dsl.*;
import ecg.Data;

public class Compress {

	public static final int BLOCK_SIZE = 10;

	public static Query<Integer,Integer> delta() {
		return new Query<Integer,Integer>() {
			private Integer prev;
			private boolean seen;
			@Override
			public void start(Sink<Integer> sink) {
				prev = null;
				seen = false;
			}
			@Override
			public void next(Integer item, Sink<Integer> sink) {
				if (!seen) {
					sink.next(item); // first element: emit as-is (base)
					prev = item;
					seen = true;
				} else {
					int diff = item - prev;
					sink.next(diff);
					prev = item;
				}
			}
			@Override
			public void end(Sink<Integer> sink) {
				sink.end();
			}
		};
	}

	public static Query<Integer,Integer> deltaInv() {
		return new Query<Integer,Integer>() {
			private Integer prev;
			private boolean seen;
			@Override
			public void start(Sink<Integer> sink) {
				prev = null;
				seen = false;
			}
			@Override
			public void next(Integer item, Sink<Integer> sink) {
				if (!seen) {
					sink.next(item); // first element: emit as-is (base)
					prev = item;
					seen = true;
				} else {
					int value = prev + item;
					sink.next(value);
					prev = value;
				}
			}
			@Override
			public void end(Sink<Integer> sink) {
				sink.end();
			}
		};
	}

	public static Query<Integer,Integer> zigzag() {
		return new Query<Integer,Integer>() {
			@Override
			public void start(Sink<Integer> sink) {
				// nothing to do
			}
			@Override
			public void next(Integer item, Sink<Integer> sink) {
				int zigzagged = (item << 1) ^ (item >> 31);
				sink.next(zigzagged);
			}
			@Override
			public void end(Sink<Integer> sink) {
				sink.end();
			}
		};

	}

	public static Query<Integer,Integer> zigzagInv() {
		return new Query<Integer,Integer>() {
			@Override
			public void start(Sink<Integer> sink) {
				// nothing to do
			}
			@Override
			public void next(Integer item, Sink<Integer> sink) {
				int unzigzagged = (item >>> 1) ^ -(item & 1);
				sink.next(unzigzagged);
			}
			@Override
			public void end(Sink<Integer> sink) {
				sink.end();
			}
		};

	}

	public static Query<Integer,Integer> pack() {
		return new Query<Integer,Integer>() {
			private int[] buf;
			private int idx;
			@Override
			public void start(Sink<Integer> sink) {
				buf = new int[BLOCK_SIZE];
				idx = 0;
			}
			@Override
			public void next(Integer item, Sink<Integer> sink) {
				buf[idx++] = item;
				if (idx == BLOCK_SIZE) {
					flushBlock(sink);
					idx = 0;
				}
			}
			// Emit n values from buf[start..start+n) bit-packed at the given bit-width.
			private void emitPacked(Sink<Integer> sink, int start, int n, int bits) {
				if (bits == 0 || n == 0) return;
				int mask = (1 << bits) - 1;
				int bitBuffer = 0;
				int bitLen = 0;
				for (int i = start; i < start + n; i++) {
					int v = buf[i] & mask;
					bitBuffer |= (v << bitLen);
					bitLen += bits;
					while (bitLen >= 8) {
						sink.next(bitBuffer & 0xFF);
						bitBuffer >>>= 8;
						bitLen -= 8;
					}
				}
				if (bitLen > 0) {
					sink.next(bitBuffer & 0xFF);
				}
			}
			private void flushBlock(Sink<Integer> sink) {
				// Compute bit-widths for unified mode
				int maxAll = 0;
				for (int i = 0; i < BLOCK_SIZE; i++) {
					if (buf[i] > maxAll){
						maxAll = buf[i];
					}
				}
				int bitsAll = 32 - Integer.numberOfLeadingZeros(maxAll);
				int unifiedBytes = 1 + (bitsAll * BLOCK_SIZE + 7) / 8;

				// Compute bit-widths for split mode
				int bitsBase = 32 - Integer.numberOfLeadingZeros(buf[0]);
				int maxDelta = 0;
				for (int i = 1; i < BLOCK_SIZE; i++) {
					if (buf[i] > maxDelta) {
						maxDelta = buf[i];
					}
				}
				int bitsDelta = 32 - Integer.numberOfLeadingZeros(maxDelta);
				int baseByteCount = (bitsBase == 0) ? 0 : (bitsBase <= 8 ? 1 : 2);
                boolean splitHeaderFits = bitsBase <= 15 && bitsDelta <= 7;
                int splitBytes = splitHeaderFits
                    ? 1 + baseByteCount + (bitsDelta * (BLOCK_SIZE - 1) + 7) / 8
                    : Integer.MAX_VALUE;

				if (unifiedBytes <= splitBytes) {
					// Mode 0: unified — header bit 7 = 0, header = bitsAll
					sink.next(bitsAll);
					emitPacked(sink, 0, BLOCK_SIZE, bitsAll);
				} else {
					// Mode 1: split — header bit 7 = 1
					int header = 0x80 | (bitsBase << 3) | bitsDelta;
					sink.next(header);
					// emit base value as raw byte
					if (bitsBase > 0) {
						sink.next(buf[0] & 0xFF);
						if (bitsBase > 8) {
							sink.next((buf[0] >>> 8) & 0xFF);
						}
					}
					// emit the remaining BLOCK_SIZE-1 values bit-packed at bitsDelta
					emitPacked(sink, 1, BLOCK_SIZE - 1, bitsDelta);
				}
			}
			@Override
			public void end(Sink<Integer> sink) {
				if (idx > 0) {
					// pad remaining with zeros and flush
					for (int i = idx; i < BLOCK_SIZE; i++) buf[i] = 0;
					flushBlock(sink);
				}
				sink.end();
			}
		};

	}

	public static Query<Integer,Integer> unpack() {
		return new Query<Integer,Integer>() {
			// States:
			//   0 = expecting header
			//   1 = unified mode: reading payload bytes (all BLOCK_SIZE values)
			//   2 = split mode: reading base byte 1 (low byte)
			//   3 = split mode: reading base byte 2 (high byte, only if bitsBase > 8)
			//   4 = split mode: reading delta payload bytes
			private int state;
			private int bits;       // unified: bits per value
			private int bitsBase;   // split: bits for base value
			private int bitsDelta;  // split: bits per delta value
			private int baseValue;  // split: accumulates the base value
			private int bitBuffer;
			private int bitLen;
			private int outCount;
			private int mask;
			@Override
			public void start(Sink<Integer> sink) {
				state = 0;
				bits = 0;
				bitsBase = 0;
				bitsDelta = 0;
				baseValue = 0;
				bitBuffer = 0;
				bitLen = 0;
				outCount = 0;
				mask = 0;
			}
			@Override
			public void next(Integer item, Sink<Integer> sink) {
				int b = item & 0xFF;
				if (state == 0) {
					// header byte
					if ((b & 0x80) == 0) {
						// Mode 0: unified — header = bitsAll
						bits = b;
						if (bits == 0) {
							// all-zero block
							for (int i = 0; i < BLOCK_SIZE; i++) sink.next(0);
							// stay in state 0
						} else {
							mask = (1 << bits) - 1;
							bitBuffer = 0;
							bitLen = 0;
							outCount = 0;
							state = 1;
						}
					} else {
						// Mode 1: split — header = 0x80 | (bitsBase << 3) | bitsDelta
						bitsBase  = (b >> 3) & 0x0F;
						bitsDelta = b & 0x07;
						baseValue = 0;
						if (bitsBase == 0) {
							// base is zero; move straight to delta payload
							sink.next(0); // emit the zero base
							outCount = 1;
							if (bitsDelta == 0) {
								// all deltas are also zero
								for (int i = 1; i < BLOCK_SIZE; i++) sink.next(0);
								state = 0;
							} else {
								mask = (bitsDelta == 32) ? 0xFFFFFFFF : (1 << bitsDelta) - 1;
								bitBuffer = 0;
								bitLen = 0;
								state = 4;
							}
						} else {
							state = 2; // read base low byte
						}
					}
				} else if (state == 1) {
					// unified mode: consume payload byte
					bitBuffer |= (b << bitLen);
					bitLen += 8;
					while (bitLen >= bits && outCount < BLOCK_SIZE) {
						sink.next(bitBuffer & mask);
						bitBuffer >>>= bits;
						bitLen -= bits;
						outCount++;
					}
					if (outCount >= BLOCK_SIZE) {
						state = 0;
					}
				} else if (state == 2) {
					// split mode: base low byte
					baseValue = b;
					if (bitsBase > 8) {
						state = 3; // need high byte too
					} else {
						// base complete; emit it
						sink.next(baseValue);
						outCount = 1;
						if (bitsDelta == 0) {
							// all deltas are zero
							for (int i = 1; i < BLOCK_SIZE; i++) sink.next(0);
							state = 0;
						} else {
							mask = (bitsDelta == 32) ? 0xFFFFFFFF : (1 << bitsDelta) - 1;
							bitBuffer = 0;
							bitLen = 0;
							state = 4;
						}
					}
				} else if (state == 3) {
					// split mode: base high byte (only when bitsBase > 8)
					baseValue |= (b << 8);
					sink.next(baseValue);
					outCount = 1;
					if (bitsDelta == 0) {
						for (int i = 1; i < BLOCK_SIZE; i++) sink.next(0);
						state = 0;
					} else {
						mask = (bitsDelta == 32) ? 0xFFFFFFFF : (1 << bitsDelta) - 1;
						bitBuffer = 0;
						bitLen = 0;
						state = 4;
					}
				} else if (state == 4) {
					// split mode: consume delta payload byte
					bitBuffer |= (b << bitLen);
					bitLen += 8;
					while (bitLen >= bitsDelta && outCount < BLOCK_SIZE) {
						sink.next(bitBuffer & mask);
						bitBuffer >>>= bitsDelta;
						bitLen -= bitsDelta;
						outCount++;
					}
					if (outCount >= BLOCK_SIZE) {
						state = 0;
					}
				}
			}
			@Override
			public void end(Sink<Integer> sink) {
				sink.end();
			}
		};
	}

	public static Query<Integer,Integer> compress() {
		return Q.pipeline(delta(), zigzag(), pack());
	}

	public static Query<Integer,Integer> decompress() {
		return Q.pipeline(unpack(), zigzagInv(), deltaInv());
	}

	public static void main(String[] args) {
		System.out.println("**********************************************");
		System.out.println("***** ToyDSL & Compression/Decompression *****");
		System.out.println("**********************************************");
		System.out.println();

		System.out.println("***** Compress *****");
		{
			// from range [0,2048) to [0,256)
			Query<Integer,Integer> q1 = Q.map(x -> x / 8);
			Query<Integer,Integer> q2 = compress();
			Query<Integer,Integer> q = Q.pipeline(q1, q2);
			Iterator<Integer> it = Data.ecgStream("100-all.csv");
			Q.execute(it, q, S.lastCount());
		}
		System.out.println();

		System.out.println("***** Compress & Decompress *****");
		{
			// from range [0,2048) to [0,256)
			Query<Integer,Integer> q1 = Q.map(x -> x / 8);
			Query<Integer,Integer> q2 = compress();
			Query<Integer,Integer> q3 = decompress();
			Query<Integer,Integer> q = Q.pipeline(q1, q2, q3);
			Iterator<Integer> it = Data.ecgStream("100-all.csv");
			Q.execute(it, q, S.lastCount());
		}
		System.out.println();
	}

}
