use log::{LevelFilter, Log, Metadata, Record};

// It's annoying to use eprintln, println, etc throughout
// rustmex prelude includes println!
// let's wire that up to log
// by implementing our own logger
struct MatLabLogger;

impl Log for MatLabLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!("[{}] {}", record.level(), record.args());
        }
    }

    fn flush(&self) {}
}

// static cause set_logger requires it to live forever
static LOGGER: MatLabLogger = MatLabLogger;

// quick helper to enable logging
pub fn init_logger() {
    let level = std::env::var("RUST_LOG")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(LevelFilter::Info);

    // set_logger only succeeds once per process
    let _ = log::set_logger(&LOGGER);

    // Always update the level
    log::set_max_level(level);
}
