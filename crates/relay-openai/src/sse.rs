use crate::MAX_BUFFER_BYTES;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EventSplitter {
    line: Vec<u8>,
    data: Option<Vec<u8>>,
    limit: usize,
    over_limit: bool,
}

impl Default for EventSplitter {
    fn default() -> Self {
        Self::with_limit(MAX_BUFFER_BYTES)
    }
}

impl EventSplitter {
    pub(crate) fn with_limit(limit: usize) -> Self {
        Self {
            line: Vec::new(),
            data: None,
            limit,
            over_limit: false,
        }
    }

    pub(crate) fn overflowed(&self) -> bool {
        self.over_limit
    }

    fn drop_buffers(&mut self) {
        self.line = Vec::new();
        self.data = None;
    }

    fn exceed(&mut self) {
        self.over_limit = true;
        self.drop_buffers();
    }

    pub(crate) fn push(&mut self, bytes: &[u8]) -> Vec<Vec<u8>> {
        let mut events = Vec::new();
        if self.over_limit {
            return events;
        }
        for &byte in bytes {
            if byte != b'\n' {
                if self.line.len() >= self.limit {
                    self.exceed();
                    break;
                }
                self.line.push(byte);
                continue;
            }
            let mut line = std::mem::take(&mut self.line);
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if let Some(event) = self.end_line(&line) {
                events.push(event);
            }
            if self.over_limit {
                break;
            }
        }
        events
    }

    fn end_line(&mut self, line: &[u8]) -> Option<Vec<u8>> {
        if line.is_empty() {
            return self.data.take();
        }
        let (field, value) = match line.iter().position(|&byte| byte == b':') {
            Some(colon) => {
                let (field, rest) = line.split_at(colon);
                (field, rest.get(1..).unwrap_or_default())
            }
            None => (line, &[][..]),
        };
        if field != b"data" {
            return None;
        }
        let value = value.strip_prefix(b" ").unwrap_or(value);
        if let Some(data) = &mut self.data {
            let added = 1usize.saturating_add(value.len());
            if data.len().saturating_add(added) > self.limit {
                self.exceed();
                return None;
            }
            data.push(b'\n');
            data.extend_from_slice(value);
        } else if value.len() > self.limit {
            self.exceed();
            return None;
        } else {
            self.data = Some(value.to_vec());
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::EventSplitter;

    fn split(input: &[u8]) -> Vec<Vec<u8>> {
        EventSplitter::default().push(input)
    }

    #[test]
    fn lf_and_crlf_separate_events() {
        assert_eq!(
            split(b"data: a\n\ndata: b\r\n\r\n"),
            [b"a".to_vec(), b"b".to_vec()]
        );
    }

    #[test]
    fn comments_and_other_fields_are_ignored() {
        assert_eq!(
            split(b": keep-alive\n\nevent: x\nid: 1\ndata: a\n\n"),
            [b"a".to_vec()]
        );
    }

    #[test]
    fn multiple_data_lines_join_with_newline() {
        assert_eq!(split(b"data: a\ndata:b\n\n"), [b"a\nb".to_vec()]);
    }

    #[test]
    fn line_without_colon_is_a_field_name() {
        assert_eq!(split(b"data\n\n"), [Vec::<u8>::new()]);
    }

    #[test]
    fn blank_line_without_data_emits_nothing() {
        assert_eq!(split(b"\n\nevent: x\n\n"), Vec::<Vec<u8>>::new());
    }

    #[test]
    fn unterminated_event_stays_buffered() {
        let mut splitter = EventSplitter::default();
        assert_eq!(splitter.push(b"data: a\n"), Vec::<Vec<u8>>::new());
        assert_eq!(splitter.push(b"data: b"), Vec::<Vec<u8>>::new());
        assert_eq!(splitter.push(b"\n\n"), [b"a\nb".to_vec()]);
    }

    #[test]
    fn only_one_leading_space_is_stripped() {
        assert_eq!(split(b"data:  a\n\n"), [b" a".to_vec()]);
    }

    #[test]
    fn line_at_the_limit_is_kept() {
        let mut splitter = EventSplitter::with_limit(8);
        assert_eq!(splitter.push(b"abcdefgh\n"), Vec::<Vec<u8>>::new());
        assert!(!splitter.overflowed());
        assert_eq!(splitter.line.len(), 0);
    }

    #[test]
    fn line_over_the_limit_drops_the_buffer() {
        let mut splitter = EventSplitter::with_limit(8);
        assert_eq!(splitter.push(b"abcdefghi"), Vec::<Vec<u8>>::new());
        assert!(splitter.overflowed());
        assert_eq!(splitter.line, [] as [u8; 0]);
        assert!(splitter.data.is_none());
        assert_eq!(splitter.push(b"data: a\n\n"), Vec::<Vec<u8>>::new());
        assert!(splitter.overflowed());
    }

    #[test]
    fn events_before_an_overlong_line_are_emitted() {
        let mut splitter = EventSplitter::with_limit(8);
        let mut input = b"data: a\n\n".to_vec();
        input.extend(b"abcdefghi");
        assert_eq!(splitter.push(&input), [b"a".to_vec()]);
        assert!(splitter.overflowed());
    }

    #[test]
    fn joined_data_over_the_limit_is_dropped() {
        let mut splitter = EventSplitter::with_limit(12);
        let mut input = b"data: abcdef".to_vec();
        input.push(b'\n');
        input.extend_from_slice(b"data: abcdef");
        input.push(b'\n');
        assert_eq!(splitter.push(&input), Vec::<Vec<u8>>::new());
        assert!(splitter.overflowed());
        assert!(splitter.data.is_none());
        assert_eq!(splitter.line, [] as [u8; 0]);
    }

    #[test]
    fn joined_data_at_the_limit_is_kept() {
        let mut splitter = EventSplitter::with_limit(12);
        let mut input = b"data: abcde".to_vec();
        input.push(b'\n');
        input.extend_from_slice(b"data: abcdef");
        input.push(b'\n');
        input.push(b'\n');
        let mut expected = b"abcde".to_vec();
        expected.push(b'\n');
        expected.extend_from_slice(b"abcdef");
        assert_eq!(splitter.push(&input), [expected]);
        assert!(!splitter.overflowed());
    }
}
