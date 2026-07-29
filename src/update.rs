// ─────────────────────────────────────────────────────────────────────────────
// Update checker — fetches latest release from GitHub and notifies if newer
// ─────────────────────────────────────────────────────────────────────────────

use anyhow::Result;
use serde::Deserialize;

const REPO_OWNER: &str = "pieeg-club";
const REPO_NAME: &str = "PiEEG-local-bridge";
const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Deserialize)]
struct Release {
    tag_name: String,
    html_url: String,
}

/// Check if a newer version is available on GitHub releases.
/// Returns `Some((new_version, download_url))` if update available, `None` otherwise.
pub async fn check_for_update() -> Result<Option<(String, String)>> {
    let url = format!(
        "https://api.github.com/repos/{}/{}/releases/latest",
        REPO_OWNER, REPO_NAME
    );

    let client = reqwest::Client::builder()
        .user_agent(format!("PiEEG-Local-Bridge/{}", CURRENT_VERSION))
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    let release: Release = client.get(&url).send().await?.json().await?;

    // Strip 'v' prefix if present (v0.1.3 -> 0.1.3)
    let remote_version = release.tag_name.strip_prefix('v').unwrap_or(&release.tag_name);

    // Simple version comparison (assumes semver format)
    if is_newer_version(CURRENT_VERSION, remote_version) {
        Ok(Some((release.tag_name, release.html_url)))
    } else {
        Ok(None)
    }
}

/// Returns true if `remote` is newer than `current`.
/// Simple string comparison assuming semver: "0.1.3" < "0.2.0"
fn is_newer_version(current: &str, remote: &str) -> bool {
    let parse_version = |v: &str| -> Vec<u32> {
        v.split('.')
            .filter_map(|s| s.parse::<u32>().ok())
            .collect()
    };

    let current_parts = parse_version(current);
    let remote_parts = parse_version(remote);

    // Compare lexicographically
    remote_parts > current_parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_comparison() {
        assert!(is_newer_version("0.1.0", "0.1.1"));
        assert!(is_newer_version("0.1.9", "0.2.0"));
        assert!(is_newer_version("0.2.0", "1.0.0"));
        assert!(!is_newer_version("0.2.0", "0.1.9"));
        assert!(!is_newer_version("1.0.0", "1.0.0"));
    }
}
