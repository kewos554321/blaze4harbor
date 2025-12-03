## blaze4harbor

**blaze4harbor** is a thin wrapper CLI around `harbor` that helps you run it and quickly inspect the resulting output directory.

### Installation

Make sure you have Python 3.12 installed, then in the project root run:

```bash
uv sync
```

Or use your preferred virtualenv tool and install dependencies:

```bash
pip install -e .
```

### Usage

From the project root, run:

```bash
python main.py run <你的-harbor-參數...>
```

This script will:

- Prefer the `harbor` binary in this project’s virtualenv, falling back to the system `harbor`
- Forward all arguments passed to `main.py` directly to `harbor`
- Parse `Results written to .../result.json` from `harbor`’s output
- List files in the resulting directory with their sizes so you can quickly inspect the output

### Project structure

- `main.py`: entrypoint for `blaze4harbor`, orchestrates `harbor` invocation and result handling
- `jobs/`: example or real result data produced by `harbor`

### License

TBD

