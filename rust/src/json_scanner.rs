use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use rayon::prelude::*;
use serde_json::Value;
use walkdir::WalkDir;

// Type alias for our schema representation for clarity.
// Outer HashMap: Key is the JSON field name (e.g., "user_id").
// Inner HashMap: Key is the data type found (e.g., "String"), value is its count.
type TypeCounts = HashMap<String, usize>;
type Schema = HashMap<String, TypeCounts>;

fn main() -> anyhow::Result<()> {
    // 1. Get the target directory from command-line arguments.
    let target_dir = env::args().nth(1).ok_or_else(|| {
        anyhow::anyhow!("Please provide a directory path as an argument.")
    })?;
    
    let root_path = Path::new(&target_dir);
    if !root_path.is_dir() {
        anyhow::bail!("Provided path is not a directory.");
    }

    // 2. Use `walkdir` to find all files ending with .json.
    let json_files: Vec<_> = WalkDir::new(root_path)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "json"))
        .map(|e| e.into_path())
        .collect();

    println!("Found {} JSON files. Starting parallel analysis... 🚀", json_files.len());

    // 3. Process all files in parallel using Rayon.
    let final_schema = json_files
        .par_iter()
        .map(|path| {
            // Analyze each file. If a file fails to parse, print a warning and return an empty schema.
            analyze_file(path).unwrap_or_else(|err| {
                eprintln!("⚠️  Warning: Failed to process file {:?}: {}", path, err);
                Schema::new()
            })
        })
        // Reduce the schemas from all threads into one final schema.
        .reduce(Schema::new, merge_schemas);

    // 4. Print the aggregated results in a clean, sorted format.
    print_results(&final_schema);

    Ok(())
}

/// Parses and analyzes a single JSON file.
fn analyze_file(path: &Path) -> anyhow::Result<Schema> {
    let file = File::open(path)?;
    // Use a BufReader for efficiency, especially with larger files.
    let reader = BufReader::new(file);

    // Parse the file's JSON content into a vector of generic `Value`s.
    let data: Vec<Value> = serde_json::from_reader(reader)?;

    let mut schema = Schema::new();

    // Iterate over each object in the top-level array.
    for item in data {
        if let Value::Object(map) = item {
            // For each key-value pair in the object, record its type.
            for (key, value) in map {
                let type_name = get_value_type(&value).to_string();
                schema
                    .entry(key)
                    .or_default()
                    .entry(type_name)
                    .and_modify(|count| *count += 1)
                    .or_insert(1);
            }
        }
    }

    Ok(schema)
}

/// Merges two schema maps together. This is the 'reduce' step.
fn merge_schemas(mut acc: Schema, other: Schema) -> Schema {
    for (key, other_type_counts) in other {
        let acc_type_counts = acc.entry(key).or_default();
        for (type_name, count) in other_type_counts {
            acc_type_counts
                .entry(type_name)
                .and_modify(|c| *c += count)
                .or_insert(count);
        }
    }
    acc
}

/// Returns a string slice representing the JSON value type.
fn get_value_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "Null",
        Value::Bool(_) => "Boolean",
        Value::Number(_) => "Number",
        Value::String(_) => "String",
        Value::Array(_) => "Array",
        Value::Object(_) => "Object",
    }
}

/// Prints the final analysis results to the console.
fn print_results(schema: &Schema) {
    println!("\n--- JSON Structure Analysis Results ---");

    // Sort keys alphabetically for consistent, readable output.
    let mut sorted_keys: Vec<_> = schema.keys().collect();
    sorted_keys.sort();

    for key in sorted_keys {
        if let Some(type_counts) = schema.get(key) {
            let total_occurrences: usize = type_counts.values().sum();
            println!("\n## Key: '{}'", key);
            println!("   - **Total Occurrences**: {}", total_occurrences);
            println!("   - **Type Distribution**:");

            let mut sorted_types: Vec<_> = type_counts.iter().collect();
            sorted_types.sort_by_key(|k| k.0); // Sort by type name

            for (type_name, count) in sorted_types {
                let percentage = (*count as f64 / total_occurrences as f64) * 100.0;
                println!("     - {:<10}: {:>10} ({:.2}%)", type_name, count, percentage);
            }
        }
    }
}