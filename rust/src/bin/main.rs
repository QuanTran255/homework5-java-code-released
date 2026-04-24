use std::time::Instant;
use hw5_wnd::*;

// cargo run --release --bin main
// target/release/main
fn main() {
	println!("**********************************************");
	println!("******************** Main ********************");
	println!("**********************************************");
	println!();

	let n = 100_000_000;
	run_exact(n);
	run_apx(n);
}

fn run_exact(n: usize) {
	let mut sink = sink::SLast::new();
	let mut query = wnd_exact::WndExact::new();
	query.start(&mut sink);

	let start = Instant::now();

	let mut sum = 0;
	for item in (0..LIMIT_SAMPLE).cycle().take(n) {
		query.next(item, &mut sink);
		let last = sink.last().unwrap();
		let last = number_u32(last);
		sum += u64::from(last);
	}
	query.end(&mut sink);

	let duration = start.elapsed();
	let dur_msec = duration.as_secs_f64() * 1000.0;
	println!("Exact Algorithm:");
	println!("  sum = {}", sum);
	println!("  duration = {:.3} msec", dur_msec);
	let throughput = (n as u128) * 1_000_000_000 / duration.as_nanos();
	println!("  throughput = {} items/sec", throughput);
	println!();
}

fn run_apx(n: usize) {
	let mut sink = sink::SLast::new();
	let mut query = wnd_apx::WndApx::new();
	query.start(&mut sink);

	let start = Instant::now();

	let mut sum = 0;
	for item in (0..LIMIT_SAMPLE).cycle().take(n) {
		query.next(item, &mut sink);
		let last = sink.last().unwrap();
		let last = number_u32(last);
		sum += u64::from(last);
	}
	query.end(&mut sink);

	let duration = start.elapsed();
	let dur_msec = duration.as_secs_f64() * 1000.0;
	println!("Approximation Algorithm:");
	println!("  sum = {}", sum);
	println!("  duration = {:.3} msec", dur_msec);
	let throughput = (n as u128) * 1_000_000_000 / duration.as_nanos();
	println!("  throughput = {} items/sec", throughput);
	println!();
}
