"""Blaze4Harbor: A wrapper CLI to run harbor and upload results."""

import json
import logging
import os
import re
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

# Environment variable names
ENV_HARBOR_PATH = "HARBOR_PATH"
ENV_LOCAL_PROJECT_DIR = "BLAZE4HARBOR_LOCAL_PROJECT_DIR"

# Required upload scripts
UPLOAD_SCRIPTS = ("bigquery_upload.py", "gcs_upload.py")

# Harbor commands/args for output directory injection
# Format: single command or (command, subcommand) tuple
COMMANDS_REQUIRING_OUTPUT = ("run", ("jobs", "start"))
COMMANDS_NOT_REQUIRING_OUTPUT = ("--help",)

# Default output directory name
DEFAULT_OUTPUT_DIR = "jobs"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


def check_required_env_vars() -> None:
    """Validate all required environment variables are set."""
    required_vars = {
        ENV_HARBOR_PATH: "Path to the harbor executable",
        ENV_LOCAL_PROJECT_DIR: "Path to cloudTop local project directory",
    }

    missing_vars = [
        f"  {var}: {desc}"
        for var, desc in required_vars.items()
        if not os.environ.get(var)
    ]

    if missing_vars:
        raise EnvironmentError(
            "Missing required environment variables:\n"
            + "\n".join(missing_vars)
            + "\n\nPlease set them before running, e.g.:\n"
            f"  export {ENV_HARBOR_PATH}=/path/to/harbor\n"
            f"  export {ENV_LOCAL_PROJECT_DIR}=/path/to/blaze4harbor"
        )


def get_harbor_executable() -> str:
    """Return the harbor executable path from HARBOR_PATH env var."""
    harbor_path = os.environ.get(ENV_HARBOR_PATH)

    if not harbor_path:
        raise EnvironmentError(
            f"{ENV_HARBOR_PATH} environment variable is not set. "
            "Please set it to the path of the harbor executable, e.g.:\n"
            f"  export {ENV_HARBOR_PATH}=/path/to/harbor"
        )

    if not Path(harbor_path).exists():
        raise FileNotFoundError(
            f"Harbor executable not found at: {harbor_path}\n"
            f"Please verify the {ENV_HARBOR_PATH} environment variable is set correctly."
        )

    return harbor_path


def get_scripts_dir() -> Path:
    """Return the upload scripts directory from BLAZE4HARBOR_LOCAL_PROJECT_DIR env var."""
    project_dir = os.environ.get(ENV_LOCAL_PROJECT_DIR)

    if not project_dir:
        raise EnvironmentError(
            f"{ENV_LOCAL_PROJECT_DIR} environment variable is not set. "
            "Please set it to the cloudTop local project directory, e.g.:\n"
            f"  export {ENV_LOCAL_PROJECT_DIR}=/path/to/blaze4harbor"
        )

    project_path = Path(project_dir)
    if not project_path.exists():
        raise FileNotFoundError(
            f"Project directory not found at: {project_dir}\n"
            f"Please verify the {ENV_LOCAL_PROJECT_DIR} environment variable is set correctly."
        )

    for script in UPLOAD_SCRIPTS:
        if not (project_path / script).exists():
            raise FileNotFoundError(
                f"Required script '{script}' not found in {project_dir}\n"
                f"Please ensure the script exists in the project directory."
            )

    return project_path


def get_default_output_dir() -> Path:
    """Return the default output directory under BLAZE4HARBOR_LOCAL_PROJECT_DIR."""
    project_dir = os.environ.get(ENV_LOCAL_PROJECT_DIR)
    if not project_dir:
        raise EnvironmentError(f"{ENV_LOCAL_PROJECT_DIR} environment variable is not set.")

    output_dir = Path(project_dir) / DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def needs_output_arg(args: list[str]) -> bool:
    """Check if the harbor command requires an output directory."""
    # Check blacklist first - skip if help flags are present
    if any(arg in COMMANDS_NOT_REQUIRING_OUTPUT for arg in args):
        return False

    for cmd in COMMANDS_REQUIRING_OUTPUT:
        if isinstance(cmd, tuple):
            # Check for subcommand pattern like ("jobs", "start")
            if len(args) >= 2 and args[0] == cmd[0] and args[1] == cmd[1]:
                return True
        elif cmd in args:
            return True
    return False


def has_output_arg(args: list[str]) -> bool:
    """Check if output directory is already specified in arguments."""
    for arg in args:
        if arg in ("-o", "--output", "--jobs-dir"):
            return True
        if arg.startswith(("-o=", "--output=", "--jobs-dir=")):
            return True
    return False


def ensure_output_arg(args: list[str]) -> list[str]:
    """Add default output directory if command requires it and not specified."""
    if not needs_output_arg(args):
        return args

    if has_output_arg(args):
        return args

    default_output = get_default_output_dir()
    logger.info("No output directory specified, using default: %s", default_output)
    return args + ["-o", str(default_output)]


def run_harbor(harbor_cmd: str, harbor_args: list[str], log_path: str) -> None:
    """Run harbor command with platform-specific script wrapper."""
    if sys.platform.startswith("darwin"):
        script_cmd = ["script", "-q", log_path, harbor_cmd] + harbor_args
    elif sys.platform.startswith("linux"):
        cmd_str = " ".join(shlex.quote(a) for a in [harbor_cmd] + harbor_args)
        script_cmd = ["script", "-q", log_path, "-c", cmd_str]
    elif sys.platform.startswith("win"):
        script_cmd = [harbor_cmd] + harbor_args
    else:
        raise RuntimeError(f"Unsupported platform: {sys.platform}")

    subprocess.run(script_cmd, text=True, check=True)


def extract_results_line(log_path: str) -> str:
    """Extract the 'Results written to' line from harbor log."""
    with open(log_path, "r") as f:
        for line in f:
            if "Results written to" in line:
                return line

    raise ValueError("Could not extract results directory line from output")


def extract_results_dir(output_text: str) -> str | None:
    """Extract the results directory path from harbor output line."""
    pattern = r"Results written to\s+(.+?)/result\.json"
    match = re.search(pattern, output_text)
    return match.group(1) if match else None


def run_upload_script(script_path: Path, task_dir: Path) -> None:
    """Run an upload script via subprocess."""
    subprocess.run(
        ["python", str(script_path), str(task_dir)],
        text=True,
        check=False,
    )


def post_process_results(task_dir: Path, script_dir: Path) -> None:
    """Upload harbor results to BigQuery and GCS."""
    result_json_path = task_dir / "result.json"

    result_data = None
    if result_json_path.exists():
        with open(result_json_path, "r", encoding="utf-8") as f:
            result_data = json.load(f)
        logger.info("Loaded result.json from %s", result_json_path)
    else:
        logger.warning("result.json not found at %s", result_json_path)

    # === Phase 2.2: Upload to BigQuery ===
    print("\n=== Phase 2.2: Uploading to BigQuery ===\n")
    if result_data is not None:
        run_upload_script(script_dir / "bigquery_upload.py", task_dir)
    else:
        logger.info("Skipping BigQuery upload (no result data)")

    # === Phase 2.3: Upload to GCS ===
    print("\n=== Phase 2.3: Uploading to GCS ===\n")
    run_upload_script(script_dir / "gcs_upload.py", task_dir)


def main(argv: list[str]) -> int:
    """Run harbor and post-process results. Returns exit code."""
    check_required_env_vars()

    temp_file_path: str | None = None
    try:
        # === Phase 1: Running harbor ===
        print("\n=== Phase 1: Running harbor ===\n")
        harbor_cmd = get_harbor_executable()
        harbor_args = ensure_output_arg(argv[1:])

        with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".log") as f:
            temp_file_path = f.name

        run_harbor(harbor_cmd, harbor_args, temp_file_path)

        # === Phase 2.1: Extracting results directory ===
        print("\n=== Phase 2.1: Extracting results directory ===\n")
        results_line = extract_results_line(temp_file_path)
        results_dir = extract_results_dir(results_line)

        if not results_dir:
            logger.warning("Could not extract results directory from output")
            return 0

        logger.info("Found results directory: %s", results_dir)

        # Phase 2.2 & 2.3 are handled in post_process_results
        post_process_results(Path(results_dir), get_scripts_dir())
        return 0

    except FileNotFoundError as e:
        logger.error("%s", e)
        return 1
    except subprocess.CalledProcessError as e:
        logger.error("Harbor failed with exit code: %d", e.returncode)
        return e.returncode
    except KeyboardInterrupt:
        logger.warning("Execution interrupted by user")
        return 130
    except Exception as e:
        logger.error("Unexpected error: %s", e)
        return 1
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
