#!/usr/bin/env bash

set -euo pipefail

NIPOPPY_GLOBAL_CONFIG="${NIPOPPY_GLOBAL_CONFIG:-}"
NIPOPPY_CONTAINERS_DIR="${NIPOPPY_CONTAINERS_DIR:-}"
NIPOPPY_PIPELINES_DIR="${NIPOPPY_PIPELINES_DIR:-}"
NIPOPPY_DERIVATIVES_DIR="${NIPOPPY_DERIVATIVES_DIR:-}"
NIPOPPY_SCRATCH_DIR="${NIPOPPY_SCRATCH_DIR:-}"

usage() {
    cat <<EOF
Usage: $0 <NIPOPPY_ROOT> [BIDS_DIR]

Initialize a Nipoppy study and override some default files/directories.

Positional arguments:
  NIPOPPY_ROOT   Path to the Nipoppy root directory (required)
  BIDS_DIR       Path to the BIDS directory (optional)

Optional settings (via environment variables):
  NIPOPPY_GLOBAL_CONFIG    Path to the global config file to copy
  NIPOPPY_CONTAINERS_DIR   Path to the containers directory to symlink
  NIPOPPY_PIPELINES_DIR    Path to the pipelines directory to copy
  NIPOPPY_DERIVATIVES_DIR  Path to the parent of the derivatives directory to symlink
  NIPOPPY_SCRATCH_DIR      Path to the parent of the scratch directory to symlink

Options:
  -h, --help       Show this help message and exit
EOF
}

die() {
    printf 'ERROR: %s\n' "$1" >&2
    exit 1
}

print_step() {
    printf '===== %s =====\n' "$1"
}

log_command() {
    printf '>'
    for arg in "$@"; do
        printf ' %q' "$arg"
    done
    printf '\n'
}

run() {
    log_command "$@"
    "$@"
}

to_absolute() {
    local path="$1"
    case "$path" in
        /*) printf '%s' "$path" ;;
        ~*) printf '%s' "${path/#\~/$HOME}" ;;
        *)  printf '%s' "$PWD/$path" ;;
    esac
}

if [[ $# -eq 0 ]] && [[ -t 0 ]]; then
    usage
    exit 0
fi

for arg in "$@"; do
    case "$arg" in
        -h|--help)
            usage
            exit 0
            ;;
    esac
done

if [[ $# -lt 1 || $# -gt 2 ]]; then
    usage >&2
    exit 2
fi

nipoppy_root="$1"
bids_dir="${2:-}"

if [[ "$nipoppy_root" == "/" ]]; then
    die "Refusing to operate on the filesystem root: $nipoppy_root"
fi

dataset_name="$(basename "$nipoppy_root")"

# paths of files/directories that can be overridden by the environment variables
global_config_path="${nipoppy_root}/global_config.json"
containers_dir_path="${nipoppy_root}/containers"
pipelines_dir_path="${nipoppy_root}/pipelines"
derivatives_dir_path="${nipoppy_root}/derivatives"
scratch_dir_path="${nipoppy_root}/scratch"

if [[ -n "$bids_dir" && ! -d "$bids_dir" ]]; then
    die "BIDS directory does not exist: $bids_dir"
fi

for var_name in NIPOPPY_CONTAINERS_DIR NIPOPPY_PIPELINES_DIR; do
    value="${!var_name:-}"
    if [[ -n "$value" && ! -d "$value" ]]; then
        die "$var_name is not an existing directory: $value"
    fi
done

for var_name in NIPOPPY_DERIVATIVES_DIR NIPOPPY_SCRATCH_DIR; do
    value="${!var_name:-}"
    if [[ -n "$value" && -e "$value" && ! -d "$value" ]]; then
        die "$var_name exists but is not a directory: $value"
    fi
done

if [[ -n "$NIPOPPY_GLOBAL_CONFIG" && ! -f "$NIPOPPY_GLOBAL_CONFIG" ]]; then
    die "NIPOPPY_GLOBAL_CONFIG is not an existing file: $NIPOPPY_GLOBAL_CONFIG"
fi

if ! command -v nipoppy >/dev/null 2>&1; then
    die 'Could not find "nipoppy" executable. Make sure it is installed and available in your PATH.'
fi

if [[ -z "$NIPOPPY_GLOBAL_CONFIG" && -z "$NIPOPPY_CONTAINERS_DIR" && -z "$NIPOPPY_PIPELINES_DIR" && -z "$NIPOPPY_DERIVATIVES_DIR" && -z "$NIPOPPY_SCRATCH_DIR" ]]; then
    die "At least one optional environment variable must be set: NIPOPPY_GLOBAL_CONFIG, NIPOPPY_CONTAINERS_DIR, NIPOPPY_PIPELINES_DIR, NIPOPPY_DERIVATIVES_DIR, NIPOPPY_SCRATCH_DIR"
fi

if [[ -n "$NIPOPPY_CONTAINERS_DIR" ]]; then
    NIPOPPY_CONTAINERS_DIR="$(to_absolute "$NIPOPPY_CONTAINERS_DIR")"
fi
if [[ -n "$NIPOPPY_DERIVATIVES_DIR" ]]; then
    NIPOPPY_DERIVATIVES_DIR="$(to_absolute "$NIPOPPY_DERIVATIVES_DIR")"
fi
if [[ -n "$NIPOPPY_SCRATCH_DIR" ]]; then
    NIPOPPY_SCRATCH_DIR="$(to_absolute "$NIPOPPY_SCRATCH_DIR")"
fi

derivatives_symlink_target="${NIPOPPY_DERIVATIVES_DIR:+$NIPOPPY_DERIVATIVES_DIR/$dataset_name/derivatives}"
scratch_symlink_target="${NIPOPPY_SCRATCH_DIR:+$NIPOPPY_SCRATCH_DIR/$dataset_name/scratch}"

echo "========== Parameters =========="
echo "Nipoppy root:               $nipoppy_root"
echo "BIDS directory:             ${bids_dir:--}"
echo "NIPOPPY_GLOBAL_CONFIG:      ${NIPOPPY_GLOBAL_CONFIG:--}"
echo "NIPOPPY_CONTAINERS_DIR:     ${NIPOPPY_CONTAINERS_DIR:--}"
echo "NIPOPPY_PIPELINES_DIR:      ${NIPOPPY_PIPELINES_DIR:--}"
echo "NIPOPPY_DERIVATIVES_DIR:    ${NIPOPPY_DERIVATIVES_DIR:--}"
echo "NIPOPPY_SCRATCH_DIR:        ${NIPOPPY_SCRATCH_DIR:--}"
echo "Derivatives symlink target: ${derivatives_symlink_target:--}"
echo "Scratch symlink target:     ${scratch_symlink_target:--}"
echo "================================"

print_step "Initializing the Nipoppy study"
init_command=(nipoppy init --dataset "$nipoppy_root")
if [[ -n "$bids_dir" ]]; then
    init_command+=(--bids-source "$bids_dir")
fi
run "${init_command[@]}"

if [[ -n "$NIPOPPY_GLOBAL_CONFIG" ]]; then
    print_step "Overriding global config file"
    run rm -f "$global_config_path"
    run cp "$NIPOPPY_GLOBAL_CONFIG" "$global_config_path"
fi

if [[ -n "$NIPOPPY_CONTAINERS_DIR" ]]; then
    print_step "Overriding containers directory"
    run rm -rf "$containers_dir_path"
    run ln -s "$NIPOPPY_CONTAINERS_DIR" "$containers_dir_path"
fi

if [[ -n "$NIPOPPY_PIPELINES_DIR" ]]; then
    print_step "Overriding pipelines directory"
    run rm -rf "$pipelines_dir_path"
    run cp -r "$NIPOPPY_PIPELINES_DIR" "$pipelines_dir_path"
fi

if [[ -n "$NIPOPPY_DERIVATIVES_DIR" ]]; then
    print_step "Overriding derivatives directory"
    run mkdir -p "$derivatives_symlink_target"
    run rm -rf "$derivatives_dir_path"
    run ln -s "$derivatives_symlink_target" "$derivatives_dir_path"
fi

if [[ -n "$NIPOPPY_SCRATCH_DIR" ]]; then
    print_step "Overriding scratch directory"
    run mkdir -p "$scratch_symlink_target"
    for subdir in "$scratch_dir_path"/*; do
        if [[ -d "$subdir" ]]; then
            subdir_name="$(basename "$subdir")"
            run mv "$subdir" "$scratch_symlink_target"
        fi
    done
    run rm -rf "$scratch_dir_path"
    run ln -s "$scratch_symlink_target" "$scratch_dir_path"
fi
