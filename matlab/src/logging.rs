use log::{LevelFilter, Log, Metadata, Record};

// It's annoying to use eprintln, println, etc throughout
// to get matlab
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
// TODO: make log level configurable
pub fn init_logger() {
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Debug))
        .ok();
}
