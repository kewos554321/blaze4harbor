import subprocess
import sys
import os
import re
import tempfile
import shlex
import json
from pathlib import Path


def check_required_env_vars() -> None:
    """Check that all required environment variables are set before execution.

    Raises:
        EnvironmentError: If any required environment variable is missing.
    """
    required_vars = {
        "HARBOR_PATH": "Path to the harbor executable",
        "BLAZE4HARBOR_LOCAL_PROJECT_DIR": "Path to cloudTop local project directory",
    }

    missing_vars = []
    for var, description in required_vars.items():
        if not os.environ.get(var):
            missing_vars.append(f"  {var}: {description}")

    if missing_vars:
        raise EnvironmentError(
            "Missing required environment variables:\n" +
            "\n".join(missing_vars) +
            "\n\nPlease set them before running, e.g.:\n"
            "  export HARBOR_PATH=/path/to/harbor\n"
            "  export BLAZE4HARBOR_LOCAL_PROJECT_DIR=/path/to/blaze4harbor"
        )


def main(argv: list[str]) -> int:
    """Run the harbor command with CLI arguments and post-process its results.

    Returns:
        Exit code: 0 for success, non-zero for failure.
    """
    # Check required environment variables before starting
    check_required_env_vars()

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

            script_dir = get_scripts_dir()

            if result_data is not None:
                # Invoke bigquery_upload.py via subprocess
                bigquery_upload_script = script_dir / "bigquery_upload.py"
                subprocess.run(
                    [sys.executable, str(bigquery_upload_script), str(task_dir)],
                    text=True,
                    check=False
                )

            # Invoke gcs_upload.py via subprocess
            gcs_upload_script = script_dir / "gcs_upload.py"
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
        return 1
    except subprocess.CalledProcessError as e:
        print(f"\nfailed with exit code: {e.returncode}", file=sys.stderr)
        return e.returncode
    except KeyboardInterrupt:
        print("\n\nExecution interrupted by user", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1
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
    """Return the path or name of the harbor executable to use.

    The path must be provided via the HARBOR_PATH environment variable.
    """
    harbor_path = os.environ.get("HARBOR_PATH")

    if not harbor_path:
        raise EnvironmentError(
            "HARBOR_PATH environment variable is not set. "
            "Please set it to the path of the harbor executable, e.g.:\n"
            "  export HARBOR_PATH=/path/to/harbor"
        )

    if not Path(harbor_path).exists():
        raise FileNotFoundError(
            f"Harbor executable not found at: {harbor_path}\n"
            "Please verify the HARBOR_PATH environment variable is set correctly."
        )

    return harbor_path


def get_scripts_dir() -> Path:
    """Return the path to the directory containing upload scripts.

    The path must be provided via the BLAZE4HARBOR_LOCAL_PROJECT_DIR environment variable.
    This should point to the cloudTop local project directory containing
    bigquery_upload.py and gcs_upload.py.
    """
    project_dir = os.environ.get("BLAZE4HARBOR_LOCAL_PROJECT_DIR")

    if not project_dir:
        raise EnvironmentError(
            "BLAZE4HARBOR_LOCAL_PROJECT_DIR environment variable is not set. "
            "Please set it to the cloudTop local project directory, e.g.:\n"
            "  export BLAZE4HARBOR_LOCAL_PROJECT_DIR=/path/to/blaze4harbor"
        )

    project_path = Path(project_dir)
    if not project_path.exists():
        raise FileNotFoundError(
            f"Project directory not found at: {project_dir}\n"
            "Please verify the BLAZE4HARBOR_LOCAL_PROJECT_DIR environment variable is set correctly."
        )

    # Verify required scripts exist
    required_scripts = ["bigquery_upload.py", "gcs_upload.py"]
    for script in required_scripts:
        if not (project_path / script).exists():
            raise FileNotFoundError(
                f"Required script '{script}' not found in {project_dir}"
            )

    return project_path


def extract_results_dir(output_text):
    """Extract the results directory path from harbor output."""
    pattern = r"Results written to\s+(.+?)/result\.json"
    match = re.search(pattern, output_text)
    
    if match:
        return match.group(1)
    return None


if __name__ == "__main__":
    sys.exit(main(sys.argv))
