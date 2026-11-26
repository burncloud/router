use std::io::Write;
use std::sync::{Arc, Mutex, PoisonError};

#[derive(Clone, Default)]
pub(crate) struct LogBuf(Arc<Mutex<Vec<u8>>>);

impl Write for LogBuf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(b);
        Ok(b.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
    type Writer = LogBuf;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl LogBuf {
    pub(crate) fn take(&self) -> String {
        let bytes = std::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner));
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::LogBuf;
    use std::io::Write;

    #[test]
    fn take_drains_the_buffer() -> std::io::Result<()> {
        let buf = LogBuf::default();
        buf.clone().write_all(b"line\n")?;
        assert_eq!(buf.take(), "line\n");
        assert_eq!(buf.take(), "");
        Ok(())
    }
}
