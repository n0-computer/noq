use core::str;
use std::{
    io::{self, Write},
    print,
};

pub(crate) fn subscribe() -> tracing::subscriber::DefaultGuard {
    let builder = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing::Level::TRACE.into())
                .from_env_lossy(),
        )
        .without_time()
        .with_line_number(true)
        .with_writer(|| TestWriter);
    tracing::subscriber::set_default(builder.finish())
}

struct TestWriter;

impl Write for TestWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        print!(
            "{}",
            str::from_utf8(buf).expect("tried to log invalid UTF-8")
        );
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        io::stdout().flush()
    }
}
