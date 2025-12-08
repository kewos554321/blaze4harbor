import subprocess
import sys
import os
import re
import tempfile
import shlex
import json
from pathlib import Path


def main(argv: list[str]) -> None:
    """Run the harbor command with CLI arguments and post-process its results."""
    temp_file_path = None
    try:
        harbor_cmd = get_harbor_executable()
        harbor_args = argv[1:]
        cmd = [harbor_cmd] + harbor_args
        
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.log') as temp_file:
            temp_file_path = temp_file.name

        if sys.platform.startswith("darwin"):
            script_cmd = ["script", "-q", temp_file_path, harbor_cmd] + harbor_args
            subprocess.run(script_cmd, text=True, check=True)
        elif sys.platform.startswith("linux"):
            cmd_str = " ".join(shlex.quote(a) for a in [harbor_cmd] + harbor_args)
            script_cmd = ["script", "-q", temp_file_path, "-c", cmd_str]
            subprocess.run(script_cmd, text=True, check=True)
        elif sys.platform.startswith("win"):
            cmd = [harbor_cmd] + harbor_args
            subprocess.run(cmd, text=True, check=True)
        else:
            raise RuntimeError(f"Unsupported platform for script/harbor wrapper: {sys.platform}")
        
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
                # Invoke bigquery_upload.py via subprocess
                bigquery_upload_script = "bigquery_upload.py"
                subprocess.run(
                    [sys.executable, str(bigquery_upload_script), str(task_dir)],
                    text=True,
                    check=False
                )

            # Invoke gcs_upload.py via subprocess
            gcs_upload_script = "gcs_upload.py"
            subprocess.run(
                [sys.executable, str(gcs_upload_script), str(task_dir)],
                text=True,
                check=False
            )
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



def extract_results_dir(output_text):
    """Extract the results directory path from harbor output."""
    pattern = r"Results written to\s+(.+?)/result\.json"
    match = re.search(pattern, output_text)
    
    if match:
        return match.group(1)
    return None


if __name__ == "__main__":
    main(sys.argv)
