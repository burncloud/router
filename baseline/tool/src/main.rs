//! 旧中转基线的驱动工具。
//!
//! - `run-twice`：构建 `harness`，在全新 SQLite 上各跑一次，比较两次结果的 `compared` 段。
//! - `perf N`：release 构建 `harness`，跑 N 次 perf 探针。
//! - `compare A.json B.json`：只比较 `compared` 段，一致时退出码 0，不一致时 1。
//!
//! 结果写在 `baseline/results/`，不入库。

mod diff;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use anyhow::{bail, ensure, Context};
use serde_json::Value;

const OLD_PACKAGE: &str = "burncloud-router";
const OLD_SOURCE_PREFIX: &str = "git+https://github.com/burncloud/burncloud?rev=";
const USAGE: &str = "usage: baseline-tool run-twice | perf N | compare A.json B.json";

struct Old {
    rev: String,
    fixture: PathBuf,
}

fn baseline_dir() -> anyhow::Result<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .context("tool 目录没有上级目录")
}

fn cargo() -> Command {
    Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
}

fn run(cmd: &mut Command) -> anyhow::Result<()> {
    let status = cmd.status().with_context(|| format!("无法启动 {cmd:?}"))?;
    ensure!(status.success(), "{cmd:?} 失败：{status}");
    Ok(())
}

fn is_sha(s: &str) -> bool {
    s.len() == 40
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn pinned_rev(source: &str) -> anyhow::Result<String> {
    let rest = source
        .strip_prefix(OLD_SOURCE_PREFIX)
        .with_context(|| format!("{OLD_PACKAGE} 不是 burncloud/burncloud 的 rev 依赖：{source}"))?;
    let (rev, resolved) = rest
        .split_once('#')
        .with_context(|| format!("{OLD_PACKAGE} 的 source 没有解析出的提交：{source}"))?;
    ensure!(
        is_sha(rev) && rev == resolved,
        "{OLD_PACKAGE} 必须钉在 40 位 SHA 上，实际是：{source}"
    );
    Ok(rev.to_owned())
}

fn locate_old(metadata: &Value) -> anyhow::Result<Old> {
    let packages = metadata["packages"]
        .as_array()
        .context("cargo metadata 没有 packages")?;
    let pkg = packages
        .iter()
        .find(|p| p["name"] == OLD_PACKAGE)
        .with_context(|| format!("cargo metadata 里没有 {OLD_PACKAGE}"))?;
    let rev = pinned_rev(pkg["source"].as_str().unwrap_or(""))?;
    let manifest = pkg["manifest_path"]
        .as_str()
        .context("cargo metadata 没有 manifest_path")?;
    let dir = Path::new(manifest)
        .parent()
        .context("manifest_path 没有上级目录")?;
    Ok(Old {
        rev,
        fixture: dir.join("tests").join("common.rs"),
    })
}

fn prepare(base: &Path) -> anyhow::Result<(Old, PathBuf)> {
    let manifest = base.join("harness").join("Cargo.toml");
    run(cargo()
        .args(["fetch", "--locked", "--manifest-path"])
        .arg(&manifest))?;
    let out = cargo()
        .args([
            "metadata",
            "--locked",
            "--offline",
            "--format-version",
            "1",
            "--manifest-path",
        ])
        .arg(&manifest)
        .output()
        .context("无法运行 cargo metadata")?;
    ensure!(out.status.success(), "cargo metadata 失败：{}", out.status);
    let old = locate_old(&serde_json::from_slice(&out.stdout)?)?;
    let target = base.join("harness").join("old");
    fs::create_dir_all(&target)?;
    fs::copy(&old.fixture, target.join("common.rs"))
        .with_context(|| format!("无法复制 {}", old.fixture.display()))?;
    fs::create_dir_all(base.join("results"))?;
    Ok((old, manifest))
}

fn build(base: &Path, manifest: &Path, release: bool) -> anyhow::Result<PathBuf> {
    let target = base.join("harness").join("target");
    let mut cmd = cargo();
    cmd.args(["build", "-q", "--locked", "--manifest-path"])
        .arg(manifest)
        .env("CARGO_TARGET_DIR", &target);
    if release {
        cmd.arg("--release");
    }
    run(&mut cmd)?;
    Ok(target
        .join(if release { "release" } else { "debug" })
        .join("old-relay-baseline"))
}

fn record(bin: &Path, old: &Old, out: &Path, log: &Path, perf: Option<u32>) -> anyhow::Result<()> {
    for p in [out, log] {
        if p.exists() {
            fs::remove_file(p)?;
        }
    }
    let mut cmd = Command::new(bin);
    cmd.arg(out).arg(log).env("OLD_COMMIT", &old.rev);
    if let Some(n) = perf {
        cmd.env("BASELINE_PERF", n.to_string());
    }
    run(&mut cmd)
}

fn compared_lines(path: &Path) -> anyhow::Result<Vec<String>> {
    let text = fs::read_to_string(path).with_context(|| format!("无法读取 {}", path.display()))?;
    let doc: Value = serde_json::from_str(&text)?;
    let compared = doc
        .get("compared")
        .with_context(|| format!("{} 没有 compared 段", path.display()))?;
    Ok(serde_json::to_string_pretty(compared)?
        .lines()
        .map(str::to_owned)
        .collect())
}

fn compare(a: &Path, b: &Path) -> anyhow::Result<(bool, String)> {
    let la = compared_lines(a)?;
    let lb = compared_lines(b)?;
    Ok(
        match diff::unified(&la, &lb, &a.display().to_string(), &b.display().to_string()) {
            Some(d) => (false, d),
            None => (
                true,
                format!("IDENTICAL (compared section, {} lines)\n", la.len()),
            ),
        },
    )
}

fn run_twice() -> anyhow::Result<bool> {
    let base = baseline_dir()?;
    let (old, manifest) = prepare(&base)?;
    let bin = build(&base, &manifest, false)?;
    let results = base.join("results");
    for i in 1..=2 {
        record(
            &bin,
            &old,
            &results.join(format!("run{i}.json")),
            &results.join(format!("run{i}.router.log")),
            None,
        )?;
    }
    let (same, report) = compare(&results.join("run1.json"), &results.join("run2.json"))?;
    print!("{report}");
    fs::write(results.join("diff.txt"), &report)?;
    Ok(same)
}

fn perf(n: u32) -> anyhow::Result<bool> {
    ensure!(n > 0, "N 必须大于 0");
    let base = baseline_dir()?;
    let (old, manifest) = prepare(&base)?;
    let bin = build(&base, &manifest, true)?;
    let results = base.join("results");
    record(
        &bin,
        &old,
        &results.join("perf-release.json"),
        &results.join("perf-release.router.log"),
        Some(n),
    )?;
    Ok(true)
}

fn dispatch(args: &[String]) -> anyhow::Result<bool> {
    match args {
        [cmd] if cmd == "run-twice" => run_twice(),
        [cmd, n] if cmd == "perf" => perf(n.parse().with_context(|| format!("N 不是正整数：{n}"))?),
        [cmd, a, b] if cmd == "compare" => {
            let (same, report) = compare(Path::new(a), Path::new(b))?;
            print!("{report}");
            Ok(same)
        }
        _ => bail!("{USAGE}"),
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match dispatch(&args) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(e) => {
            eprintln!("{e:?}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{compare, dispatch, locate_old, pinned_rev};
    use serde_json::json;
    use std::path::Path;

    const SHA: &str = "69aedf5867b072344427a3f78ec70a7524161755";

    #[test]
    fn rev_must_be_full_and_resolved_to_itself() {
        let ok = format!("git+https://github.com/burncloud/burncloud?rev={SHA}#{SHA}");
        assert_eq!(pinned_rev(&ok).ok().as_deref(), Some(SHA));
        let short = "git+https://github.com/burncloud/burncloud?rev=69aedf5#69aedf5867b072344427a3f78ec70a7524161755";
        assert!(pinned_rev(short).is_err());
        let branch = "git+https://github.com/burncloud/burncloud?branch=main#69aedf5867b072344427a3f78ec70a7524161755";
        assert!(pinned_rev(branch).is_err());
        let other = format!(
            "git+https://github.com/burncloud/burncloud?rev={SHA}#{}",
            "0".repeat(40)
        );
        assert!(pinned_rev(&other).is_err());
        assert!(pinned_rev("registry+https://github.com/rust-lang/crates.io-index").is_err());
    }

    #[test]
    fn fixture_path_comes_from_the_router_package() -> anyhow::Result<()> {
        let meta = json!({"packages": [
            {"name": "tokio", "source": "registry", "manifest_path": "/x/tokio/Cargo.toml"},
            {"name": "burncloud-router",
             "source": format!("git+https://github.com/burncloud/burncloud?rev={SHA}#{SHA}"),
             "manifest_path": "/c/69aedf5/crates/traffic/router/Cargo.toml"}
        ]});
        let old = locate_old(&meta)?;
        assert_eq!(old.rev, SHA);
        assert_eq!(
            old.fixture,
            Path::new("/c/69aedf5/crates/traffic/router/tests/common.rs")
        );
        assert!(locate_old(&json!({"packages": []})).is_err());
        Ok(())
    }

    #[test]
    fn compare_ignores_not_compared_and_key_order() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("baseline-tool-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let a = dir.join("a.json");
        let b = dir.join("b.json");
        let c = dir.join("c.json");
        std::fs::write(
            &a,
            r#"{"compared":{"x":1,"y":[2]},"not_compared":{"t":1.5}}"#,
        )?;
        std::fs::write(&b, r#"{"not_compared":{"t":9},"compared":{"y":[2],"x":1}}"#)?;
        std::fs::write(&c, r#"{"compared":{"x":2,"y":[2]}}"#)?;
        let (same, report) = compare(&a, &b)?;
        assert!(same);
        assert!(report.starts_with("IDENTICAL (compared section, "));
        let (same, report) = compare(&a, &c)?;
        assert!(!same);
        assert!(report.contains("-  \"x\": 1,"));
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn bad_arguments_are_rejected() {
        assert!(dispatch(&[]).is_err());
        assert!(dispatch(&["perf".into(), "x".into()]).is_err());
        assert!(dispatch(&["compare".into(), "a".into()]).is_err());
    }
}
