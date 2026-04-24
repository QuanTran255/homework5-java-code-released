use super::*;

const MEM_SIZE: usize = 900; // DO NOT CHANGE
const CODE_BITS: usize = 7;
const CODE_MASK: u16 = 0x7f;

// Layout inside ram:
// bytes 0..875   : packed 7-bit circular buffer
// bytes 876..877 : head
// bytes 878..879 : filled
// bytes 880..883 : running sum
const BUFFER_BYTES: usize = 876;
const HEAD_AT: usize = BUFFER_BYTES;
const FILLED_AT: usize = HEAD_AT + 2;
const SUM_AT: usize = FILLED_AT + 2;

// Quantized representatives. For every input x, encode(x) chooses the
// largest representative q <= x. These representatives were chosen so
// that q >= 0.95 * x for every x in 0..1024.
const DECODE: [u16; 89] = [
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
	21, 23, 25, 27, 29, 31, 33, 35, 37, 39,
	42, 45, 48, 51, 54, 57, 61, 65, 69, 73,
	77, 82, 87, 92, 97, 103, 109, 115, 122, 129,
	136, 144, 152, 161, 170, 179, 189, 199, 210, 222,
	234, 247, 261, 275, 290, 306, 323, 341, 359, 378,
	398, 419, 442, 466, 491, 517, 545, 574, 605, 637,
	671, 707, 745, 785, 827, 871, 917, 966, 1017,
];

// This file contains the implementation of an approximation algorithm
// for the sliding-window average.
//
// We have to make sure that we do not use more than `MEM_SIZE`
// bytes of memory for the state of our streaming algorithm.
//

// State of the streaming algorithm
pub struct WndApx {
	// DO NOT MAKE ANY CHANGE HERE
	ram: [u8; MEM_SIZE], // memory contents
	// DO NOT MAKE ANY CHANGE HERE
}


impl WndApx {

	pub fn new() -> Self {
		// DO NOT MAKE ANY CHANGE HERE
		Self {
			ram: [0; MEM_SIZE],
		}
		// DO NOT MAKE ANY CHANGE HERE
	}

	// Return the largest representative q <= x. This is the 7-bit code
	// stored in the packed circular buffer.
	fn encode(x: u16) -> u8 {
		let mut lo = 0;
		let mut hi = DECODE.len();
		while lo + 1 < hi {
			let mid = (lo + hi) / 2;
			if DECODE[mid] <= x {
				lo = mid;
			} else {
				hi = mid;
			}
		}
		lo as u8
	}

	// The assignment forces all mutable state into ram, so the scalar
	// fields are stored manually using little-endian byte order.
	fn get_u16(&self, at: usize) -> u16 {
		u16::from_le_bytes([self.ram[at], self.ram[at + 1]])
	}

	fn set_u16(&mut self, at: usize, x: u16) {
		let b = x.to_le_bytes();
		self.ram[at] = b[0];
		self.ram[at + 1] = b[1];
	}

	fn get_u32(&self, at: usize) -> u32 {
		u32::from_le_bytes([
			self.ram[at],
			self.ram[at + 1],
			self.ram[at + 2],
			self.ram[at + 3],
		])
	}

	fn set_u32(&mut self, at: usize, x: u32) {
		let b = x.to_le_bytes();
		self.ram[at] = b[0];
		self.ram[at + 1] = b[1];
		self.ram[at + 2] = b[2];
		self.ram[at + 3] = b[3];
	}

	fn head(&self) -> u16 {
		self.get_u16(HEAD_AT)
	}

	fn set_head(&mut self, x: u16) {
		self.set_u16(HEAD_AT, x);
	}

	fn filled(&self) -> u16 {
		self.get_u16(FILLED_AT)
	}

	fn set_filled(&mut self, x: u16) {
		self.set_u16(FILLED_AT, x);
	}

	fn sum(&self) -> u32 {
		self.get_u32(SUM_AT)
	}

	fn set_sum(&mut self, x: u32) {
		self.set_u32(SUM_AT, x);
	}

	// Read/write the 7-bit code at logical buffer index `index`.
	// The extra buffer byte lets this safely read across byte boundaries.
	fn read7(&self, index: usize) -> u8 {
		let bit = index * CODE_BITS;
		let byte = bit / 8;
		let shift = bit % 8;
		let word = u16::from(self.ram[byte])
			| (u16::from(self.ram[byte + 1]) << 8);
		((word >> shift) & CODE_MASK) as u8
	}

	fn write7(&mut self, index: usize, code: u8) {
		let bit = index * CODE_BITS;
		let byte = bit / 8;
		let shift = bit % 8;
		let word = u16::from(self.ram[byte])
			| (u16::from(self.ram[byte + 1]) << 8);
		let mask = CODE_MASK << shift;
		let word = (word & !mask) | (u16::from(code) << shift);
		self.ram[byte] = word as u8;
		self.ram[byte + 1] = (word >> 8) as u8;
	}

}

impl Query for WndApx {

	fn start<S: Sink>(&mut self, _sink: &mut S) {
		self.ram = [0; MEM_SIZE];
	}

	fn next<S: Sink>(&mut self, item: u16, sink: &mut S) {
		assert!(item < LIMIT_SAMPLE);

		let head = self.head() as usize;
		let filled = self.filled();
		let mut sum = self.sum();

		// The initial window is padded with zeros. Once full, evict the
		// quantized value currently stored at the circular-buffer head.
		let old_q = if filled < WND_SIZE as u16 {
			self.set_filled(filled + 1);
			0
		} else {
			let old_code = self.read7(head);
			DECODE[old_code as usize] as u32
		};

		// Store the quantized new item and update the running approximate sum.
		let new_code = Self::encode(item);
		let new_q = DECODE[new_code as usize] as u32;
		self.write7(head, new_code);

		sum = sum + new_q - old_q;
		self.set_sum(sum);
		self.set_head(((head + 1) % WND_SIZE) as u16);

		// Output sum / 1000 as integer and fractional parts.
		sink.next((
			(sum / WND_SIZE as u32) as u16,
			(sum % WND_SIZE as u32) as u16,
		));
	}

	fn end<S: Sink>(&mut self, sink: &mut S) {
		sink.end();
	}

}

// cargo test -- --nocapture --test-threads=1
// cargo test --release -- --nocapture test_wnd_apx_0
// cargo test --release -- --nocapture test_wnd_apx_1
// cargo test --release -- --nocapture test_wnd_apx_2
// cargo test --release -- --nocapture test_wnd_apx_3
#[cfg(test)]
mod tests {
	use super::*;
	use crate::wnd_exact::WndExact;

	#[test]
	fn test_wnd_apx_3() {
		println!("\n");
		println!("***** Approximate Algorithm for Sliding Average *****");
		println!();

		let mut max_rel_error = 0.0_f64;
		for value in 0..LIMIT_SAMPLE {
			let mut sink = sink::SLast::new();
			let mut query = WndExact::new();
			let mut sink_apx = sink::SLast::new();
			let mut query_apx = WndApx::new();
			query.start(&mut sink);
			query_apx.start(&mut sink_apx);

			let it = {
				// constant stream
				core::iter::repeat(value).take(1000).enumerate()
			};
			for (i, item) in it {
				println!("i = {}, item = {}", i, item);
				query.next(item, &mut sink);
				let last = sink.last().unwrap();
				let last = number_u32(last);
				query_apx.next(item, &mut sink_apx);
				let last_apx = sink_apx.last().unwrap();
				let last_apx = number_u32(last_apx);
				let abs_error = last - last_apx;
				println!(
					"  sum: value = {}, estimate = {}, abs. error = {}",
					last, last_apx, abs_error
				);
				assert!(K * abs_error <= last);
				let wnd_size = u16::try_from(WND_SIZE).unwrap();
				let wnd_size = f64::from(wnd_size);
				let last = f64::from(last) / wnd_size;
				let last_apx = f64::from(last_apx) / wnd_size;
				let rel_error = 100.0 * (last - last_apx) / last;
				println!(
					"  avg: value = {:.3}, estimate = {:.3}, rel. error = {:.2}%",
					last, last_apx, rel_error
				);
				if last > 0.0 {
					assert!(rel_error <= EPS_P + 0.000001);
				}
				if !rel_error.is_nan() {
					max_rel_error = max_rel_error.max(rel_error);
				}
				println!();
			}
			query.end(&mut sink);
			query_apx.end(&mut sink_apx);
		}

		println!("maximum relative error = {}", max_rel_error);
		println!();
	}
	
	#[test]
	fn test_wnd_apx_2() {
		println!("\n");
		println!("***** Approximate Algorithm for Sliding Average *****");
		println!();

		let mut sink = sink::SLast::new();
		let mut query = WndExact::new();
		let mut sink_apx = sink::SLast::new();
		let mut query_apx = WndApx::new();
		query.start(&mut sink);
		query_apx.start(&mut sink_apx);

		let n = 10_000;
		let it = {
			(0..LIMIT_SAMPLE).cycle().take(n).enumerate()
		};
		let mut max_rel_error = 0.0_f64;
		for (i, item) in it {
			println!("i = {}, item = {}", i, item);
			query.next(item, &mut sink);
			let last = sink.last().unwrap();
			let last = number_u32(last);
			query_apx.next(item, &mut sink_apx);
			let last_apx = sink_apx.last().unwrap();
			let last_apx = number_u32(last_apx);
			let abs_error = last - last_apx;
			println!(
				"  sum: value = {}, estimate = {}, abs. error = {}",
				last, last_apx, abs_error
			);
			assert!(K * abs_error <= last);
			let wnd_size = u16::try_from(WND_SIZE).unwrap();
			let wnd_size = f64::from(wnd_size);
			let last = f64::from(last) / wnd_size;
			let last_apx = f64::from(last_apx) / wnd_size;
			let rel_error = 100.0 * (last - last_apx) / last;
			println!(
				"  avg: value = {:.3}, estimate = {:.3}, rel. error = {:.2}%",
				last, last_apx, rel_error
			);
			if last > 0.0 {
				assert!(rel_error <= EPS_P + 0.000001);
			}
			if !rel_error.is_nan() {
				max_rel_error = max_rel_error.max(rel_error);
			}
			println!();
		}
		query.end(&mut sink);
		query_apx.end(&mut sink_apx);

		println!("maximum relative error = {:.2}", max_rel_error);
		println!();
	}
	
	#[test]
	fn test_wnd_apx_1() {
		println!("\n");
		println!("***** Approximate Algorithm for Sliding Average *****");
		println!();

		// Used in the reference solution for testing individual components
		// of the algorithm.
	}

	#[test]
	fn test_wnd_apx_0() {
		println!("\n");
		println!("***** Approximate Algorithm for Sliding Average *****");
		println!();

		let name = core::any::type_name::<WndApx>();
		let size = core::mem::size_of::<WndApx>();
		assert_eq!(size, MEM_SIZE);
		println!("size of {} = {} bytes", name, size);
		println!();
	}
	
}
