use super::{Output, number_f64};

pub trait Sink {
	fn next(&mut self, item: Output);
	fn end(&mut self);
}

pub struct SPrinter {
	// empty struct
}

impl SPrinter {

	pub fn new() -> Self {
		Self { }
	}

}

impl Sink for SPrinter {

	fn next(&mut self, item: Output) {
		println!("{}", number_f64(item));
	}

	fn end(&mut self) {
		println!("END");
	}

}

pub struct SLast {
	last: Option<Output>,
}

impl SLast {

	pub fn new() -> Self {
		Self { last: None }
	}

	pub fn last(&self) -> Option<Output> {
		self.last
	}

}

impl Sink for SLast {

	fn next(&mut self, item: Output) {
		self.last = Some(item);
	}

	fn end(&mut self) {
		// do nothing
	}

}

// cargo test -- --nocapture --test-threads=1
// cargo test --release -- --nocapture test_sink_1
#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_sink_1() {
		println!("\n");
		println!("***** Use the Printer Sink *****");
		println!();

		let mut sink = SPrinter::new();
		sink.next((10, 100));
		sink.next((20, 50));
		sink.next((30, 10));
		sink.end();
	}

}
