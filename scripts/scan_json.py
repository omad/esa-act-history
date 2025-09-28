import json
import sys
from pathlib import Path
from collections import defaultdict
from multiprocessing import Pool
from functools import reduce
from typing import Any

# Type alias for our schema representation, similar to the Rust version.
# {"field_name": {"type_name": count}}
TypeCounts = dict[str, int]
Schema = dict[str, TypeCounts]

def get_value_type(value: Any) -> str:
    """Returns a string representation of a JSON value's type."""
    if value is None:
        return "Null"
    if isinstance(value, bool):
        return "Boolean"
    # In JSON, int and float are just "Number".
    if isinstance(value, (int, float)):
        return "Number"
    if isinstance(value, str):
        return "String"
    if isinstance(value, list):
        return "Array"
    if isinstance(value, dict):
        return "Object"
    # Fallback for any other unexpected types.
    return "Unknown"

def analyze_file(path: Path) -> Schema:
    """
    Parses a single JSON file and returns its schema.
    
    Each file is expected to be a JSON array of objects.
    """
    # Using nested defaultdict simplifies counting.
    schema: dict[str, defaultdict[str, int]] = defaultdict(lambda: defaultdict(int))
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if not isinstance(data, list):
            print(f"⚠️  Warning: File {path.name} is not a JSON array, skipping.", file=sys.stderr)
            return {}

        for item in data:
            if isinstance(item, dict):
                for key, value in item.items():
                    type_name = get_value_type(value)
                    schema[key][type_name] += 1
    
    except json.JSONDecodeError:
        print(f"⚠️  Warning: Could not decode JSON from {path.name}, skipping.", file=sys.stderr)
        return {}
    except Exception as e:
        print(f"⚠️  Warning: An error occurred with file {path.name}: {e}", file=sys.stderr)
        return {}

    # Convert defaultdicts to regular dicts for stable merging.
    return {key: dict(type_counts) for key, type_counts in schema.items()}

def merge_schemas(acc: Schema, other: Schema) -> Schema:
    """Merges two schema dictionaries. This is the 'reduce' step."""
    for key, other_type_counts in other.items():
        acc_type_counts = acc.setdefault(key, {})
        for type_name, count in other_type_counts.items():
            acc_type_counts[type_name] = acc_type_counts.get(type_name, 0) + count
    return acc

def print_results(schema: Schema):
    """Prints the final analysis results to the console."""
    print("\n--- JSON Structure Analysis Results ---")
    
    # Sort keys alphabetically for consistent, readable output.
    for key in sorted(schema.keys()):
        type_counts = schema[key]
        total_occurrences = sum(type_counts.values())
        
        print(f"\n## Key: '{key}'")
        print(f"   - **Total Occurrences**: {total_occurrences}")
        print(f"   - **Type Distribution**:")
        
        for type_name in sorted(type_counts.keys()):
            count = type_counts[type_name]
            percentage = (count / total_occurrences) * 100
            # Format for clean alignment.
            print(f"     - {type_name:<10}: {count:>10} ({percentage:.2f}%)")

def main():
    """Main function to orchestrate the file scanning and analysis."""
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <directory>", file=sys.stderr)
        sys.exit(1)

    target_dir = Path(sys.argv[1])
    if not target_dir.is_dir():
        print(f"Error: '{target_dir}' is not a valid directory.", file=sys.stderr)
        sys.exit(1)

    # 1. Discover all .json files recursively.
    print(f"Searching for JSON files in '{target_dir}'...")
    json_files = list(target_dir.rglob("*.json"))
    
    if not json_files:
        print("No JSON files found.")
        return

    print(f"Found {len(json_files)} JSON files. Starting parallel analysis... 🚀")

    # 2. Use a process pool to analyze files in parallel.
    # The 'with' statement ensures the pool is properly closed.
    with Pool() as pool:
        # map_async + get() is used to allow for keyboard interrupt (Ctrl+C).
        # imap_unordered is great here as it processes results as they complete.
        results = pool.imap_unordered(analyze_file, json_files)
        
        # 3. Reduce the results from all processes into a single schema.
        final_schema = reduce(merge_schemas, results, {})

    # 4. Print the final aggregated results.
    if final_schema:
        print_results(final_schema)
    else:
        print("\nAnalysis complete, but no valid object structures were found.")

if __name__ == "__main__":
    # This check is essential for multiprocessing to work correctly.
    main()

