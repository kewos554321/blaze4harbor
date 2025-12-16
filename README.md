# Blaze4Harbor

A wrapper CLI to run harbor from g3 and automatically upload results to BigQuery and GCS.

## Setup & Usage

### Step 1: Copy project to local (cloudTop)

Copy the blaze4harbor project from g3 to your local cloudTop directory:

```bash
cp -r /path/to/g3/blaze4harbor ~/blaze4harbor_local
```

The copied project should contain:
- `bigquery_upload.py`
- `gcs_upload.py`
- `pyproject.toml`

### Step 2: Install dependencies in local project

Navigate to the copied project directory and set up the Python environment:

```bash
cd ~/blaze4harbor_local

# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment
uv venv

# Activate virtual environment
source .venv/bin/activate

# Install dependencies (harbor, google-cloud-storage, google-cloud-bigquery)
uv sync
```

**Note**: Harbor may have environment compatibility issues. It is recommended to use version 0.1.18:

```bash
# Install specific harbor version
uv pip install harbor==0.1.18
```

Verify packages are installed:

```bash
# Check installed packages
uv pip list | grep -E "harbor|google-cloud"
# Expected output:
# google-cloud-bigquery    3.x.x
# google-cloud-storage     2.x.x
# harbor                   0.1.18
```

After installation, get the required paths:

```bash
# Get harbor executable path (ensure you're in venv)
uv run which harbor
# Example output: /home/user/blaze4harbor_local/.venv/bin/harbor

# Get project directory path
pwd
# Example output: /home/user/blaze4harbor_local
```

Save these two paths for the next step.

### Step 3: Set environment variables in g3

Navigate to the blaze4harbor directory in g3 and set the environment variables:

```bash
cd /path/to/g3/blaze4harbor

# Set harbor executable path
export HARBOR_PATH=/home/user/blaze4harbor_local/.venv/bin/harbor

# Set local project directory path
export BLAZE4HARBOR_LOCAL_PROJECT_DIR=/home/user/blaze4harbor_local
```

### Step 4: Run blaze4harbor

Harbor arguments should be passed after `--`:

```bash
# Show help
blaze run :blaze4harbor -- --help

# Show run command help
blaze run :blaze4harbor -- run --help

# Run a benchmark
blaze run :blaze4harbor -- run \
  -d hello-world@head \
  --agent gemini-cli \
  --ak version=v0.18.0-preview.0 \
  --model google/gemini-3-pro-preview \
  --debug \
  -k 2 \
  --n-concurrent 2 \
  -o /usr/local/google/home/user/Documents/harbor_test_loc/jobs
```

## Default Output Directory

Due to environment differences between g3 and cloudTop, harbor's output directory must be on the local cloudTop filesystem.

> **Warning**: Do not use g3 paths as the output directory. Harbor runs on cloudTop and will have permission issues writing to g3 filesystem. Always use a local cloudTop path.

**Blaze4Harbor will automatically set the output directory** if you don't specify `-o` (or `--jobs-dir`):

| Command | Auto `-o` |
|---------|-----------|
| `run` | ✅ |
| `jobs start` | ✅ |
| `jobs resume` | ❌ (uses existing job path) |
| `sweeps run` | ❌ (uses config file) |

When `-o` is not specified, the default output directory is:
```
$BLAZE4HARBOR_LOCAL_PROJECT_DIR/jobs
```

Example:
```bash
# Without -o: automatically uses default output directory
blaze run :blaze4harbor -- run -d hello-world@head --agent gemini-cli ...
# Output: INFO: No output directory specified, using default: /home/user/blaze4harbor_local/jobs

# With -o: uses your specified directory
blaze run :blaze4harbor -- run -d hello-world@head --agent gemini-cli -o /my/custom/path ...
```

## What happens after running

Blaze4Harbor will:

1. **Phase 1**: Run harbor with the specified arguments
2. **Phase 2.1**: Extract the results directory from harbor output
3. **Phase 2.2**: Upload results to BigQuery and GCS
   - Upload `result.json` data to BigQuery
   - Upload all files in the task directory to GCS

## Environment Variables

| Variable | Description |
|----------|-------------|
| `HARBOR_PATH` | Path to the harbor executable in local venv |
| `BLAZE4HARBOR_LOCAL_PROJECT_DIR` | Path to the local project directory containing upload scripts |
