import subprocess
import sys
import os
import re
import tempfile
import shlex
import json
from pathlib import Path


def main():
    """Run the harbor command with CLI arguments and post-process its results."""
    temp_file_path = None
    try:
        harbor_cmd = get_harbor_executable()
        harbor_args = normalize_jobs_dir_args(sys.argv[1:])
        cmd = [harbor_cmd] + harbor_args
        
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.log') as temp_file:
            temp_file_path = temp_file.name

        script_cmd = ["script", "-q", temp_file_path] + cmd
        
        subprocess.run(
            script_cmd,
            text=True,
            check=True,
        )
        
        print("\n=== Harbor finished, starting post-processing ===")

        results_line = extract_results_line(temp_file_path)
        results_dir = extract_results_dir(results_line)

        if results_dir:
            print(f"\nFound results directory: {results_dir}")

            task_dir = Path(results_dir)
            result_json_path = task_dir / "result.json"

            result_data = None
            if result_json_path.exists():
                with open(result_json_path, "r", encoding="utf-8") as f:
                    result_data = json.load(f)
                print(f"Loaded result.json from {result_json_path}")
            else:
                print(f"Warning: result.json not found at {result_json_path}", file=sys.stderr)

            if result_data is not None:
                upload_result_to_bigquery_stub(result_data, task_dir)

            upload_task_dir_to_gcs_stub(task_dir)
        else:
            print("\nWarning: could not extract results directory from output", file=sys.stderr)
        
        return 0
    except FileNotFoundError:
        print("Error: 'harbor' command not found. Please ensure it is installed.", file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"\nfailed with exit code: {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)
    except KeyboardInterrupt:
        print("\n\nExecution interrupted by user", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)


def extract_results_line(temp_file_path: str) -> str:
    """Return the line from harbor output that contains the results path."""
    key_line = None
    with open(temp_file_path, 'r') as f:
        for line in f:
            if 'Results written to' in line:
                key_line = line
                break

    if key_line:
        return key_line

    raise Exception("Could not extract results directory line from output")


def get_harbor_executable() -> str:
    """Return the path or name of the harbor executable to use."""
    venv_bin = Path(__file__).parent / ".venv" / "bin" / "harbor"

    if venv_bin.exists():
        return str(venv_bin)

    import shutil
    system_harbor = shutil.which("harbor")
    if system_harbor:
        return system_harbor

    raise FileNotFoundError("harbor executable not found in venv or system PATH")


def normalize_jobs_dir_args(argv: list[str]) -> list[str]:
    """Ensure -o/--jobs-dir points under the user's home, or add a default jobs dir."""
    home = os.path.expanduser("~")
    has_jobs_dir = False

    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg in ("-o", "--jobs-dir"):
            has_jobs_dir = True
            if i + 1 < len(argv):
                original = argv[i + 1]
                argv[i + 1] = os.path.join(home, original)
            break
        i += 1

    if not has_jobs_dir:
        argv = list(argv)
        argv.extend(["-o", os.path.join(home, "jobs")])

    return argv


def upload_result_to_bigquery_stub(result_data: dict, task_dir: Path) -> None:
    """Placeholder: upload result.json data to BigQuery (not yet implemented)."""
    # TODO: Implement BigQuery upload logic.
    print(f"[stub] Would upload result.json for task at {task_dir} to BigQuery.")

    # Print a short preview of the result data for inspection.
    try:
        preview = json.dumps(result_data, indent=2, ensure_ascii=False)
    except TypeError:
        preview = str(result_data)

    max_len = 800
    if len(preview) > max_len:
        print("\nresult.json preview (truncated):")
        print(preview[:max_len] + "\n... (truncated)")
    else:
        print("\nresult.json preview:")
        print(preview)


def upload_task_dir_to_gcs_stub(task_dir: Path) -> None:
    """Placeholder: upload all task directory contents to GCS (not yet implemented)."""
    # TODO: Implement GCS upload logic.
    print(f"[stub] Would upload all contents of {task_dir} to GCS.")


def extract_results_dir(output_text):
    """Extract the results directory path from harbor output."""
    pattern = r"Results written to\s+(.+?)/result\.json"
    match = re.search(pattern, output_text)
    
    if match:
        return match.group(1)
    return None


if __name__ == "__main__":
    main()
