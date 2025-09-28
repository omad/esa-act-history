use anyhow::{Context, Result};
use clap::Parser;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::process::Command;
use tracing::{error, info, warn};

// Constants for default values or common strings
const DEFAULT_OUTPUT_DIR: &str = "/tmp/esa-feeds";
const DEFAULT_FILE_TO_EXTRACT: &str = "feed.json";
const JJ_COMMAND_NAME: &str = "jj";

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
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing for structured logging.
    // This will print info/warn/error messages to stderr by default.
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let output_dir = &args.output_dir;
    let file_to_extract = &args.file_to_extract;
    // Determine the 'jj' executable path, falling back to just "jj" if not specified.
    let jj_executable = args.jj_path.as_deref().unwrap_or(Path::new(JJ_COMMAND_NAME));

    info!("Ensuring output directory exists: {:?}", output_dir);
    // Create the output directory and all its parents if they don't exist.
    fs::create_dir_all(output_dir)
        .await
        .context(format!("Failed to create output directory {:?}", output_dir))?;

    info!("Fetching commit history using `{}`...", jj_executable.display());
    let commits = get_commit_history(jj_executable).await?;
    info!("Found {} commits to process.", commits.len());

    let mut tasks = Vec::new();
    for (commit_id, timestamp) in commits {
        // Clone necessary data for each async task.
        // PathBufs and Strings are cheap to clone for this purpose.
        let output_dir_clone = output_dir.clone();
        let file_to_extract_clone = file_to_extract.clone();
        let jj_executable_clone = jj_executable.to_path_buf(); // Clone Path to move into task

        // Spawn a new asynchronous task for each commit.
        tasks.push(tokio::spawn(async move {
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

    // Wait for all spawned tasks to complete and aggregate their results.
    let mut successful_extractions = 0;
    let mut failed_extractions = 0;

    for task_handle in tasks {
        match task_handle.await {
            // Task completed (Ok(_)) and the inner Result was Ok
            Ok(Ok(_)) => {
                successful_extractions += 1;
            }
            // Task completed (Ok(_)) but the inner Result was Err
            Ok(Err(e)) => {
                error!("Extraction task failed: {:?}", e);
                failed_extractions += 1;
            }
            // Task itself failed to join (e.g., panicked)
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

/// Fetches the commit history using `jj log` and parses its output.
/// Returns a vector of (commit_id, timestamp) pairs.
async fn get_commit_history(jj_executable: &Path) -> Result<Vec<(String, String)>> {
    let output = Command::new(jj_executable)
        .arg("log")
        .arg("--no-graph")
        .arg("-r")
        .arg("root()..@") // All commits from the root to the current commit (head)
        .arg("-T")
        // Format string: commit_id space timestamp newline
        .arg(r#"concat(commit_id, " ", self.author().timestamp().format("%s"), "\n")"#)
        .output()
        .await
        .context(format!("Failed to run `{} log`", jj_executable.display()))?;

    // Check if the command executed successfully.
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "`{} log` failed with status: {}. Stderr: {}",
            jj_executable.display(),
            output.status,
            stderr
        );
    }

    // Convert stdout bytes to a UTF-8 string.
    let stdout = String::from_utf8(output.stdout)
        .context("`jj log` output is not valid UTF-8")?;

    // Parse each line into a (commit_id, timestamp) tuple.
    let commits: Vec<(String, String)> = stdout
        .lines()
        .filter_map(|line| {
            let parts: Vec<&str> = line.trim().splitn(2, ' ').collect();
            if parts.len() == 2 {
                // Return (commit_id, timestamp) as owned Strings
                Some((parts[0].to_string(), parts[1].to_string()))
            } else if !line.trim().is_empty() {
                // Log a warning for malformed lines but don't fail the whole process.
                warn!("Skipping malformed `jj log` line: '{}'", line);
                None
            } else {
                None // Skip empty lines
            }
        })
        .collect();

    Ok(commits)
}

/// Extracts the content of `file_to_extract` for a given commit and writes it to a file
/// in the specified output directory.
async fn extract_file_for_commit(
    jj_executable: &Path,
    commit_id: &str,
    timestamp: &str,
    file_to_extract: &str,
    output_path: &Path,
) -> Result<()> {
    // Construct the output filename: e.g., "1678886400_abcd12345.json"
    let output_file_name = format!("{}_{}.json", timestamp, commit_id);
    let output_file_path = output_path.join(output_file_name);

    info!(
        "Extracting '{}' for commit {} (timestamp {}) to {:?}",
        file_to_extract, commit_id, timestamp, output_file_path
    );

    // Construct the file specification for `jj file show`.
    let file_spec = format!("root-file:\"{}\"", file_to_extract);

    let output = Command::new(jj_executable)
        .arg("file")
        .arg("show")
        .arg("-r")
        .arg(commit_id) // Specify the revision
        .arg(file_spec) // Specify the file
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

    // Convert stdout bytes (file content) to a UTF-8 string.
    let file_content = String::from_utf8(output.stdout)
        .context(format!(
            "Content of '{}' for commit {} is not valid UTF-8",
            file_to_extract, commit_id
        ))?;

    // Write the extracted content to the output file.
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