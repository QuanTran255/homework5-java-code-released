use super::*;

// This file contains the implementation of an exact algorithm for the
// sliding-window average.
//
// We assume that the window is initialized with 0s.
// So, the partial windows in the beginning are completed with 0s.
//

// State of the streaming algorithm
pub struct WndExact {
	buffer: [u16; WND_SIZE], // array for ring buffer
	index: usize, // position for placing the next element
		// Invariant: 0 <= index < WND_SIZE
	sum: usize, // sum of elements in the window
}

impl WndExact {

	pub fn new() -> Self {
		Self {
			buffer: [0; WND_SIZE],
			index: 0,
			sum: 0,
		}
	}

}

impl Query for WndExact {

	fn start<S: Sink>(&mut self, _sink: &mut S) {
		self.buffer = [0; WND_SIZE];
		self.index = 0;
		self.sum = 0;
	}

	fn next<S: Sink>(&mut self, item: u16, sink: &mut S) {
		assert!(item < LIMIT_SAMPLE);

		// we insert the current element & we evict the oldest ome
		let old = self.buffer[self.index];
		self.buffer[self.index] = item;
		self.index += 1;
		if self.index == WND_SIZE {
			self.index = 0;
		}
		self.sum += usize::from(item);
		self.sum -= usize::from(old);

		let q = self.sum / WND_SIZE;
		let q = u16::try_from(q).unwrap();
		let r = self.sum % WND_SIZE;
		let r = u16::try_from(r).unwrap();
		sink.next((q, r));
	}

	fn end<S: Sink>(&mut self, sink: &mut S) {
		sink.end();
	}

}

// cargo test -- --nocapture --test-threads=1
// cargo test --release -- --nocapture test_wnd_exact_0
// cargo test --release -- --nocapture test_wnd_exact_1
// cargo test --release -- --nocapture test_wnd_exact_2
#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_wnd_exact_2() {
		println!("\n");
		println!("***** Exact Algorithm for Sliding Average *****");
		println!();

		let input = Vec::from_iter(
			(0..LIMIT_SAMPLE).cycle().take(100 * WND_SIZE)
		);

		let mut sink = sink::SLast::new();
		let mut query = WndExact::new();
		query.start(&mut sink);

		for (j, &item) in input.iter().enumerate() {
			query.next(item, &mut sink);
			let last = sink.last().unwrap();
			let i = if j < WND_SIZE {
				0
			} else {
				j - WND_SIZE + 1
			};
			let sum = number_u32(last);
			let expected = {
				(i..=j)
				.map(|idx| u32::from(input[idx]))
				.sum()
			};
			assert_eq!(sum, expected);
			//println!("sum = {}", sum);
		}
		query.end(&mut sink);
	}
	
	#[test]
	fn test_wnd_exact_1() {
		println!("\n");
		println!("***** Exact Algorithm for Sliding Average *****");
		println!();

		let mut sink = sink::SPrinter::new();
		let mut query = WndExact::new();
		query.start(&mut sink);
		for item in 1..=10 {
			query.next(item, &mut sink);
		}
		query.end(&mut sink);
		println!();
	}

	#[test]
	fn test_wnd_exact_0() {
		println!("\n");
		println!("***** Exact Algorithm for Sliding Average *****");
		println!();

		let name = core::any::type_name::<WndExact>();
		let size = core::mem::size_of::<WndExact>();
		println!("size of {} = {} bytes", name, size);
		println!();
	}
	
}
