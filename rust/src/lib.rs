pub mod sink;
use sink::Sink;

pub mod wnd_exact;
pub mod wnd_apx;

// window size 1000 (1 sec for a 1 KHz signal)
pub const WND_SIZE: usize = 1000;

// sensor measurements in range 0..LIMIT_SAMPLE
pub const LIMIT_SAMPLE: u16 = 1024; 

pub const K: u32 = 20; // approximation parameter K
pub const EPS: f64 = 0.05; // epsilon = 1 / K = 0.05
pub const EPS_P: f64 = 5.0; // 5.0% relative error

// Type `Output` of output values of the form `(y1, y2)`,
// where `y2` should satisfy `0 <= y2 < WND_SIZE`.
// The pair represents the number `y = y1 + (y2 / WND_SIZE)`.
type Output = (u16, u16);

pub fn number_f64(item: Output) -> f64 {
	let (y1, y2) = item;
	let y1 = f64::from(y1);
	let y2 = f64::from(y2);
	let y = y1 + (y2 / WND_SIZE as f64);
	y
}

pub fn number_u32(item: Output) -> u32 {
	let (y1, y2) = item;
	let y1 = u32::from(y1);
	let y2 = u32::from(y2);
	let wnd_size = u32::try_from(WND_SIZE).unwrap();
	let y = wnd_size * y1 + y2;
	y
}

pub trait Query {
	fn start<S: Sink>(&mut self, sink: &mut S);
	fn next<S: Sink>(&mut self, item: u16, sink: &mut S);
	fn end<S: Sink>(&mut self, sink: &mut S);
}
