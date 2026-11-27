use std::fmt::Write;

const CONTEXT: usize = 3;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Op {
    Same,
    Del,
    Add,
}

fn ops(old: &[String], new: &[String]) -> Vec<(Op, usize, usize)> {
    let mut lcs = vec![vec![0_u32; new.len() + 1]; old.len() + 1];
    for oi in (0..old.len()).rev() {
        for ni in (0..new.len()).rev() {
            lcs[oi][ni] = if old[oi] == new[ni] {
                lcs[oi + 1][ni + 1] + 1
            } else {
                lcs[oi + 1][ni].max(lcs[oi][ni + 1])
            };
        }
    }
    let (mut oi, mut ni) = (0, 0);
    let mut out = Vec::new();
    while oi < old.len() || ni < new.len() {
        if oi < old.len() && ni < new.len() && old[oi] == new[ni] {
            out.push((Op::Same, oi, ni));
            oi += 1;
            ni += 1;
        } else if oi < old.len() && (ni == new.len() || lcs[oi + 1][ni] >= lcs[oi][ni + 1]) {
            out.push((Op::Del, oi, ni));
            oi += 1;
        } else {
            out.push((Op::Add, oi, ni));
            ni += 1;
        }
    }
    out
}

pub(crate) fn unified(a: &[String], b: &[String], name_a: &str, name_b: &str) -> Option<String> {
    let ops = ops(a, b);
    let changed: Vec<usize> = ops
        .iter()
        .enumerate()
        .filter(|(_, (op, _, _))| *op != Op::Same)
        .map(|(k, _)| k)
        .collect();
    let first = *changed.first()?;
    let mut hunks: Vec<(usize, usize)> = Vec::new();
    let mut start = first.saturating_sub(CONTEXT);
    let mut end = first;
    for &k in &changed {
        if k > end + 2 * CONTEXT {
            hunks.push((start, (end + CONTEXT).min(ops.len() - 1)));
            start = k.saturating_sub(CONTEXT);
        }
        end = k;
    }
    hunks.push((start, (end + CONTEXT).min(ops.len() - 1)));
    let mut out = format!("--- {name_a}\n+++ {name_b}\n");
    for (s, e) in hunks {
        let slice = &ops[s..=e];
        let (_, a0, b0) = slice[0];
        let a_len = slice.iter().filter(|(op, _, _)| *op != Op::Add).count();
        let b_len = slice.iter().filter(|(op, _, _)| *op != Op::Del).count();
        let _ = writeln!(out, "@@ -{},{a_len} +{},{b_len} @@", a0 + 1, b0 + 1);
        for &(op, i, j) in slice {
            let _ = match op {
                Op::Same => writeln!(out, " {}", a[i]),
                Op::Del => writeln!(out, "-{}", a[i]),
                Op::Add => writeln!(out, "+{}", b[j]),
            };
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::unified;

    fn lines(s: &str) -> Vec<String> {
        s.lines().map(str::to_owned).collect()
    }

    #[test]
    fn identical_inputs_have_no_diff() {
        let a = lines("x\ny");
        assert_eq!(unified(&a, &a, "a", "b"), None);
    }

    #[test]
    fn changed_line_is_reported_with_context() {
        let a = lines("1\n2\n3\n4\n5\n6\n7\n8");
        let b = lines("1\n2\n3\n4\nfive\n6\n7\n8");
        let d = unified(&a, &b, "a", "b").unwrap_or_default();
        assert_eq!(
            d,
            "--- a\n+++ b\n@@ -2,7 +2,7 @@\n 2\n 3\n 4\n-5\n+five\n 6\n 7\n 8\n"
        );
    }

    #[test]
    fn distant_changes_make_two_hunks() {
        let a: Vec<String> = (0..30).map(|i| i.to_string()).collect();
        let mut b = a.clone();
        b[2] = "x".into();
        b[25] = "y".into();
        let d = unified(&a, &b, "a", "b").unwrap_or_default();
        assert_eq!(d.matches("@@ -").count(), 2);
    }

    #[test]
    fn insertion_at_end() {
        let d = unified(&lines("a"), &lines("a\nb"), "x", "y").unwrap_or_default();
        assert_eq!(d, "--- x\n+++ y\n@@ -1,1 +1,2 @@\n a\n+b\n");
    }
}
