# generate_bids_subcohorts

Identify and export sub-cohorts from a [Nipoppy](https://nipoppy.readthedocs.io) BIDS dataset based on flexible, JSON-driven filter specifications. Filters can combine:

- **Datatype presence** (e.g. must have `anat` AND `dwi`)
- **Suffix presence** (e.g. must have `T1w` AND `T2w`)
- **Scanner metadata** from JSON sidecars (e.g. `Manufacturer == Siemens`)
- **Protocol counts** (e.g. exactly 1 `B700` DWI run)

---

## Installation

Dependencies are listed in `requirements.txt`. Choose one of the setup methods below.

### Option A — `uv` (recommended)

[uv](https://docs.astral.sh/uv/) creates and manages the virtual environment automatically.

```bash
# Create a virtual environment and install dependencies
uv venv .venv
uv pip install -r requirements.txt
```

Activate when needed:

```bash
source .venv/bin/activate
```

### Option B — standard `venv`

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Quickstart

### 1. Prepare a filter specification file

Create a JSON file (e.g. `my_filter_spec.json`) with one or more named filters. See [Filter Specification](#filter-specification) below for the full schema and `sample_bids_filter_spec.json` for concrete examples.

### 2. Run the script

```bash
python generate_bids_subcohorts.py \
    --ds_path /path/to/nipoppy_dataset \
    --bids_filter_spec_file my_filter_spec.json \
    --bids_filter_spec_name complete_multi_shell_dwi \
    --output_dir /path/to/output
```

On first run the script indexes the BIDS dataset (via `bids2table`) and saves intermediate tables to `output_dir/`. Use `--read_bids_df` and/or `--read_metadata_df` on subsequent runs to skip re-indexing:

```bash
python generate_bids_subcohorts.py \
    --ds_path /path/to/nipoppy_dataset \
    --read_bids_df \
    --read_metadata_df \
    --bids_filter_spec_file my_filter_spec.json \
    --bids_filter_spec_name complete_multi_shell_dwi \
    --output_dir /path/to/output
```

### 3. Outputs

| File | Description |
|------|-------------|
| `output_dir/bids2table_index.tsv` | Full BIDS index table (cached) |
| `output_dir/bids2table_metadata.tsv` | Scanner metadata table (cached) |
| `output_dir/<filter_name>/count_table.tsv` | Per-subject/session protocol counts |
| `output_dir/<filter_name>/filtered_participants.tsv` | Participants passing all criteria |
| `output_dir/<filter_name>/participants_<ses>.txt` | One text file per session listing participant IDs |

---

## CLI Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--ds_path` | Yes | Path to the root Nipoppy dataset (must contain a `bids/` subdirectory) |
| `--bids_filter_spec_file` | Yes | Path to the JSON filter specification file |
| `--bids_filter_spec_name` | Yes | Name of the filter to apply (top-level key in the JSON) |
| `--output_dir` | Yes | Directory for output files |
| `--read_bids_df` | No | Load the BIDS index from a previously saved TSV in the output dir instead of re-indexing |
| `--read_metadata_df` | No | Load the scanner metadata from a previously saved TSV in the output dir instead of re-extracting |

---

## Filter Specification

Filters are defined in a JSON file as named objects. See the following sample files for concrete examples:

| File | Contents |
|------|----------|
| [`sample_bids_filter_spec.json`](sample_bids_filter_spec.json) | Scanner-agnostic DWI filters |
| [`scanner_bids_filter_spec.json`](scanner_bids_filter_spec.json) | Scanner-specific DWI filters (Philips, GE, Siemens) |
| [`anat_bids_filter_spec.json`](anat_bids_filter_spec.json) | Multi-modal anatomical filters |

Each filter supports:

- **`description`** — human-readable label
- **`scanner_metadata.sidecar_tags`** — list of T1w JSON sidecar tags to extract
- **`groupby_cols`** / **`count_cols`** — columns for building the protocol count table
- **`criteria`** — the filtering logic:
  - **`scanner_metadata`** — filter by sidecar tag values; omit or leave `{}` to skip
  - **`datatypes`** — `values` (list) + `match` (`"AND"` / `"OR"`)
  - **`suffixes`** — `values` (list) + `match` (`"AND"` / `"OR"`); omit entire key to skip
  - **`count_spec`** — `count_operator` (`"equal_to"` or `"greater_or_equal_to"`) + `rules` (list of column/threshold dicts)

### Match gates (`AND` / `OR`)

| Key | AND (default) | OR |
|-----|---------------|----|
| `datatypes.match` | Participant must have **all** listed datatypes | Participant must have **at least one** |
| `suffixes.match` | Participant must have **all** listed suffixes | Participant must have **at least one** |

### `count_spec` logic

Rules are row-level AND conditions on the count table. A participant passes if they satisfy **any one** rule (OR across rules). The `count_operator` controls the threshold comparison:

| Value | Behaviour |
|---|---|
| `"greater_or_equal_to"` (default) | `n_<col> >= value` |
| `"equal_to"` | `n_<col> == value` |

---

## Examples

### Single-shell DWI (any manufacturer)

```bash
python generate_bids_subcohorts.py \
    --ds_path /data/my_study \
    --bids_filter_spec_file sample_bids_filter_spec.json \
    --bids_filter_spec_name complete_single_shell_dwi \
    --output_dir /data/my_study/subcohorts
```

### Siemens-only single-shell DWI

```bash
python generate_bids_subcohorts.py \
    --ds_path /data/my_study \
    --bids_filter_spec_file scanner_bids_filter_spec.json \
    --bids_filter_spec_name Siemens_single_shell_dwi \
    --output_dir /data/my_study/subcohorts
```

### Multi-modal anatomical (exactly 1 T1w AND 1 FLAIR)

```bash
python generate_bids_subcohorts.py \
    --ds_path /data/my_study \
    --bids_filter_spec_file anat_bids_filter_spec.json \
    --bids_filter_spec_name exact_one_T1w_and_FLAIR \
    --output_dir /data/my_study/subcohorts
```
