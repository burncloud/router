#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct EventSplitter {
    line: Vec<u8>,
    data: Option<Vec<u8>>,
}

impl EventSplitter {
    pub(crate) fn push(&mut self, bytes: &[u8]) -> Vec<Vec<u8>> {
        let mut events = Vec::new();
        for &byte in bytes {
            if byte != b'\n' {
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
        match &mut self.data {
            Some(data) => {
                data.push(b'\n');
                data.extend_from_slice(value);
            }
            None => self.data = Some(value.to_vec()),
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
}
