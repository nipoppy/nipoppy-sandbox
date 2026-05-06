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
| `--read_bids_df` | No | Load the BIDS index from a previously saved TSV instead of re-indexing |
| `--read_metadata_df` | No | Load the scanner metadata from a previously saved TSV instead of re-extracting |

---

### Match gates (`AND` / `OR`)

| Key | Applies to | AND (default) | OR |
|-----|-----------|---------------|----|
| `datatype_match` | `datatypes` list | Participant must have **all** listed datatypes | Participant must have **at least one** |
| `suffix_match` | `suffixes` list | Participant must have **all** listed suffixes | Participant must have **at least one** |

### `count_spec` logic

Each object in `count_spec` is a row-level AND filter applied to the count table. A participant/session row only needs to satisfy **one** of the objects (OR across items). The threshold comparison is controlled by `force_exact_counts`:

- `false` (default): `n_<col> >= value`
- `true`: `n_<col> == value`

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
    --bids_filter_spec_file sample_bids_filter_spec.json \
    --bids_filter_spec_name Siemens_single_shell_dwi \
    --output_dir /data/my_study/subcohorts
```

### Multi-modal anatomical (T1w AND T2w AND FLAIR)

```bash
python generate_bids_subcohorts.py \
    --ds_path /data/my_study \
    --bids_filter_spec_file sample_bids_filter_spec.json \
    --bids_filter_spec_name complete_multi_modal_anat \
    --output_dir /data/my_study/subcohorts
```
