use anyhow::{Context, Result};
use clap::Parser;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::fs;
use tokio::process::Command;
use tokio::sync::Semaphore; // Import Semaphore
use tracing::{error, info, warn};

// Constants for default values or common strings
const DEFAULT_OUTPUT_DIR: &str = "/tmp/esa-feeds";
const DEFAULT_FILE_TO_EXTRACT: &str = "feed.json";
const JJ_COMMAND_NAME: &str = "jj";
const DEFAULT_CONCURRENCY_LIMIT: usize = 50; // Sensible default limit

/// Extract all versions of a specific file from a jj repository into separate files.
///
/// This utility leverages `jj log` and `jj file show` to iterate through the commit
/// history and save each version of a specified file to a timestamped file in
/// an output directory.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the output directory where versions of the file will be saved.
    #[arg(short, long, default_value = DEFAULT_OUTPUT_DIR)]
    output_dir: PathBuf,

    /// The name of the file to extract from the repository.
    /// This should be a path relative to the repository root, e.g., "src/data/my_file.txt".
    #[arg(short, long, default_value = DEFAULT_FILE_TO_EXTRACT)]
    file_to_extract: String,

    /// Override the path to the 'jj' executable if it's not in your system's PATH.
    #[arg(long)]
    jj_path: Option<PathBuf>,

    /// Maximum number of concurrent file extraction tasks to run.
    /// Limits how many 'jj file show' commands are active at once to prevent
    /// "Too many open files" errors.
    #[arg(short, long, default_value_t = DEFAULT_CONCURRENCY_LIMIT)]
    concurrency_limit: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing for structured logging.
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let output_dir = &args.output_dir;
    let file_to_extract = &args.file_to_extract;
    let jj_executable = args
        .jj_path
        .as_deref()
        .unwrap_or(Path::new(JJ_COMMAND_NAME));
    let concurrency_limit = args.concurrency_limit;

    info!("Ensuring output directory exists: {:?}", output_dir);
    fs::create_dir_all(output_dir).await.context(format!(
        "Failed to create output directory {:?}",
        output_dir
    ))?;

    info!(
        "Fetching commit history using `{}`...",
        jj_executable.display()
    );
    let commits = get_commit_history(jj_executable).await?;
    info!("Found {} commits to process.", commits.len());
    info!("Concurrency limit set to {}.", concurrency_limit);

    // Create a semaphore to limit concurrent tasks.
    let semaphore = Arc::new(Semaphore::new(concurrency_limit));

    let mut tasks = Vec::new();
    for (commit_id, timestamp) in commits {
        // Acquire a permit from the semaphore. This will pause if the limit is reached.
        let semaphore = Arc::clone(&semaphore);
        let permit = semaphore.acquire_owned().await.context(
            "Failed to acquire semaphore permit. This indicates a shutdown or internal error.",
        )?;

        let output_dir_clone = output_dir.clone();
        let file_to_extract_clone = file_to_extract.clone();
        let jj_executable_clone = jj_executable.to_path_buf();

        // Spawn a new asynchronous task. The `permit` is moved into the task.
        // It will be dropped (and thus released) when the task's future completes.
        tasks.push(tokio::spawn(async move {
            let _permit = permit; // Move permit into the task scope
            extract_file_for_commit(
                &jj_executable_clone,
                &commit_id,
                &timestamp,
                &file_to_extract_clone,
                &output_dir_clone,
            )
            .await
        }));
    }

    let mut successful_extractions = 0;
    let mut failed_extractions = 0;

    for task_handle in tasks {
        match task_handle.await {
            Ok(Ok(_)) => {
                successful_extractions += 1;
            }
            Ok(Err(e)) => {
                error!("Extraction task failed: {:?}", e);
                failed_extractions += 1;
            }
            Err(e) => {
                error!("Tokio task join error: {:?}", e);
                failed_extractions += 1;
            }
        }
    }

    info!(
        "Processing complete. Successful extractions: {}, Failed extractions: {}",
        successful_extractions, failed_extractions
    );

    if failed_extractions > 0 {
        Err(anyhow::anyhow!(
            "Some files failed to extract. Please check the logs for details."
        ))
    } else {
        Ok(())
    }
}

async fn get_commit_history(jj_executable: &Path) -> Result<Vec<(String, String)>> {
    let output = Command::new(jj_executable)
        .arg("log")
        .arg("--no-graph")
        .arg("-r")
        .arg("root()..@")
        .arg("-T")
        .arg(r#"concat(commit_id, " ", self.author().timestamp().format("%s"), "\n")"#)
        .output()
        .await
        .context(format!("Failed to run `{} log`", jj_executable.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "`{} log` failed with status: {}. Stderr: {}",
            jj_executable.display(),
            output.status,
            stderr
        );
    }

    let stdout = String::from_utf8(output.stdout).context("`jj log` output is not valid UTF-8")?;

    let commits: Vec<(String, String)> = stdout
        .lines()
        .filter_map(|line| {
            let parts: Vec<&str> = line.trim().splitn(2, ' ').collect();
            if parts.len() == 2 {
                Some((parts[0].to_string(), parts[1].to_string()))
            } else if !line.trim().is_empty() {
                warn!("Skipping malformed `jj log` line: '{}'", line);
                None
            } else {
                None
            }
        })
        .collect();

    Ok(commits)
}

async fn extract_file_for_commit(
    jj_executable: &Path,
    commit_id: &str,
    timestamp: &str,
    file_to_extract: &str,
    output_path: &Path,
) -> Result<()> {
    let output_file_name = format!("{}_{}.json", timestamp, commit_id);
    let output_file_path = output_path.join(output_file_name);

    info!(
        "Extracting '{}' for commit {} (timestamp {}) to {:?}",
        file_to_extract, commit_id, timestamp, output_file_path
    );

    let file_spec = format!("root-file:\"{}\"", file_to_extract);

    let output = Command::new(jj_executable)
        .arg("file")
        .arg("show")
        .arg("-r")
        .arg(commit_id)
        .arg(file_spec)
        .output()
        .await
        .context(format!(
            "Failed to run `{} file show` for commit {}",
            jj_executable.display(),
            commit_id
        ))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "`{} file show` failed for commit {} with status: {}. Stderr: {}",
            jj_executable.display(),
            commit_id,
            output.status,
            stderr
        );
    }

    let file_content = String::from_utf8(output.stdout).context(format!(
        "Content of '{}' for commit {} is not valid UTF-8",
        file_to_extract, commit_id
    ))?;

    fs::write(&output_file_path, file_content)
        .await
        .context(format!(
            "Failed to write extracted file to {:?}",
            output_file_path
        ))?;

    info!(
        "Successfully extracted commit {} to {:?}",
        commit_id, output_file_path
    );
    Ok(())
}
