#!/usr/bin/env python

import argparse
import datetime
import json
import shutil
from pathlib import Path
from typing import Tuple

TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
PIPELINE_STEP_PATH_PREFIX = (
    "[[NIPOPPY_DPATH_PIPELINES]]/[[PIPELINE_NAME]]-[[PIPELINE_VERSION]]/"
)
PIPELINE_TYPE_TO_CONFIG_FIELD_MAP = {
    "bidsification": "BIDS_PIPELINES",
    "processing": "PROC_PIPELINES",
    "extraction": "EXTRACTION_PIPELINES",
}


def get_extra_pipelines_subdirs(dpath_pipelines: Path) -> set:
    pipeline_types = PIPELINE_TYPE_TO_CONFIG_FIELD_MAP.keys()

    if set(pipeline_types).issubset(
        found_subdirs := set(
            path.name for path in dpath_pipelines.iterdir() if path.is_dir()
        )
    ):
        return found_subdirs - set(pipeline_types)

    return set()


def check_already_migrated(dpath_dataset: Path) -> Tuple[int, int, int]:
    pipeline_types = PIPELINE_TYPE_TO_CONFIG_FIELD_MAP.keys()
    dpath_pipelines = dpath_dataset / "pipelines"

    found_already_migrated_pipelines = False

    if extra_subdirs := get_extra_pipelines_subdirs(dpath_pipelines):
        print(
            f"WARNING: Found unexpected subdirectories in {dpath_pipelines}: "
            f"{extra_subdirs}. They should be deleted."
        )

    for pipeline_type in pipeline_types:
        if (
            n_pipelines := len(
                list(dpath_pipelines.glob(f"{pipeline_type}/*/config.json"))
            )
        ) > 0:
            found_already_migrated_pipelines = True
            print(
                f"Found {n_pipelines} {pipeline_type} "
                "pipelines that have already been migrated."
            )
    return found_already_migrated_pipelines


def migrate_pipeline(
    pipeline_config: dict,
    pipeline_type: str,
    dpath_dataset: Path,
    dry_run: bool = False,
):
    pipeline_name = pipeline_config["NAME"]
    pipeline_version = pipeline_config["VERSION"]

    pipeline_config["PIPELINE_TYPE"] = pipeline_type
    pipeline_config["SCHEMA_VERSION"] = "1"

    dpath_pipeline_current = (
        dpath_dataset / "pipelines" / f"{pipeline_name}-{pipeline_version}"
    )
    # add pipeline_type subdirectory to path
    dpath_pipeline_new = (
        dpath_pipeline_current.parent / pipeline_type / dpath_pipeline_current.name
    )

    if not dpath_pipeline_current.exists():
        raise FileNotFoundError(
            f"Pipeline directory not found: {dpath_pipeline_current}"
        )
    if dpath_pipeline_new.exists():
        raise FileExistsError(
            f"Pipeline directory already exists: {dpath_pipeline_new}"
        )

    for i_step, step_config in enumerate(pipeline_config["STEPS"]):
        for field in [
            "DESCRIPTOR_FILE",
            "INVOCATION_FILE",
            "TRACKER_CONFIG_FILE",
            "PYBIDS_IGNORE_FILE",
        ]:
            if field in step_config:
                path = str(step_config[field])
                if Path(path).name != path:
                    path_stripped = path.removeprefix(PIPELINE_STEP_PATH_PREFIX)
                    if path_stripped == path:
                        print(
                            f"WARNING: Path for field {field} in step {i_step+1} "
                            "should be relative to parent directory, "
                            f"but current value is {path}"
                        )
                    else:
                        print(
                            f"Removed {PIPELINE_STEP_PATH_PREFIX} prefix for "
                            f"field {field}"
                        )
                    step_config[field] = path_stripped

    # write pipeline config to dedicated file
    fpath_pipeline_config = dpath_pipeline_current / "config.json"
    if fpath_pipeline_config.exists():
        raise FileExistsError(
            f"Pipeline config file already exists: {fpath_pipeline_config}"
        )
    pipeline_config_json = json.dumps(pipeline_config, indent=4)
    print(f"Writing pipeline config to {fpath_pipeline_config}")
    if dry_run:
        print(pipeline_config_json)
    else:
        fpath_pipeline_config.write_text(pipeline_config_json)

    # rename pipeline directory
    print(
        f"Renaming pipeline directory: {dpath_pipeline_current} -> {dpath_pipeline_new}"
    )
    if not dry_run:
        dpath_pipeline_new.parent.mkdir(parents=True, exist_ok=True)
        dpath_pipeline_current.rename(dpath_pipeline_new)


def migrate_dataset(dpath_dataset: Path, dry_run: bool = False):

    fpath_config = dpath_dataset / "global_config.json"
    if not fpath_config.exists():
        raise FileNotFoundError(f"Global config file not found: {fpath_config}")

    # check if the dataset is already migrated
    # i.e. if the pipelines directory contains subdirectories for each pipeline type
    # and no direct pipelines directories
    if check_already_migrated(dpath_dataset):
        print("The dataset seems to already have been migrated. Aborting.")
        return

    # load the config file
    config: dict = json.loads(
        fpath_config.read_text().replace("UPDATE_DOUGHNUT", "UPDATE_STATUS")
    )

    for field_to_remove in ["DATASET_NAME", "VISIT_IDS", "SESSION_IDS"]:
        if field_to_remove in config:
            print(f"Removing deprecated field {field_to_remove} from config file.")
            config.pop(field_to_remove)

    # make a backup of all original pipeline files
    dpath_pipelines = dpath_dataset / "pipelines"
    dpath_pipelines_backup = dpath_dataset / f"pipelines-{TIMESTAMP}"
    print(f"Backing up original pipelines directory to {dpath_pipelines_backup}")
    if not dry_run:
        dpath_pipelines.rename(dpath_pipelines_backup)
        shutil.copytree(dpath_pipelines_backup, dpath_pipelines)

    # migrate the pipelines
    for pipeline_type, pipeline_list_field in PIPELINE_TYPE_TO_CONFIG_FIELD_MAP.items():
        if pipeline_list_field in config:
            for pipeline_config in config[pipeline_list_field]:
                print("-" * 80)
                print(
                    f"Migrating {pipeline_type} pipeline: {pipeline_config['NAME']} "
                    f"{pipeline_config['VERSION']}"
                )
                migrate_pipeline(
                    pipeline_config, pipeline_type, dpath_dataset, dry_run=dry_run
                )
        else:
            print(f"No {pipeline_type} pipelines found in the config file.")
            continue

        # remove pipeline configs
        config.pop(pipeline_list_field)

    for extra_pipelines_subdir in get_extra_pipelines_subdirs(dpath_pipelines):
        fpath_subdir: Path = dpath_pipelines / extra_pipelines_subdir
        dname_components = str(extra_pipelines_subdir).split("-")
        if len(dname_components) > 1 and (
            len(set(fpath_subdir.glob("*descriptor*"))) > 0
            or len(set(fpath_subdir.glob("*tracker*"))) > 0
        ):
            print(
                f"Extra subdirectory found: {fpath_subdir}. This looks like an unused "
                "pipeline. Deleting."
            )
            if not dry_run:
                shutil.rmtree(fpath_subdir)
        else:
            print(
                f"WARNING: Found unexpected subdirectory {extra_pipelines_subdir} in "
                f"{dpath_pipelines}"
            )

    print("-" * 80)

    # rename original file (keep as backup)
    fname_config_backup = fpath_config.name.replace(".json", f"-{TIMESTAMP}.json")
    fpath_config_backup = dpath_dataset / fname_config_backup
    print(f"Backing up original config file to {fpath_config_backup}")
    if not dry_run:
        fpath_config.rename(fpath_config_backup)

    # write the new config file
    print(f"Writing new config file to {fpath_config}")
    config_json = json.dumps(config, indent=4)
    if dry_run:
        print(config_json)
    else:
        fpath_config.write_text(config_json)

    # rename doughnut/imaging bagel files
    fpath_doughnut_orig = dpath_dataset / "sourcedata" / "imaging" / "doughnut.tsv"
    fpath_doughnut_new = (
        dpath_dataset / "sourcedata" / "imaging" / "curation_status.tsv"
    )
    if fpath_doughnut_orig.exists():
        print(f"Renaming {fpath_doughnut_orig} -> {fpath_doughnut_new}")
        if not dry_run:
            fpath_doughnut_orig.rename(fpath_doughnut_new)
    fpath_bagel_orig = dpath_dataset / "derivatives" / "imaging_bagel.tsv"
    fpath_bagel_new = dpath_dataset / "derivatives" / "processing_status.tsv"
    if fpath_bagel_orig.exists():
        print(f"Renaming {fpath_bagel_orig} -> {fpath_bagel_new}")
        if not dry_run:
            fpath_bagel_orig.rename(fpath_bagel_new)

    print(
        f'Migration complete! Update to 0.4 ("pip install -U nipoppy") and try running '
        'a "nipoppy run" command with the "--simulate" flag to confirm'
    )


def build_parser():
    parser = argparse.ArgumentParser(
        description=(
            "Migrate a Nipoppy dataset from version 0.3 to 0.4. This will change the "
            "global config file and files in the <DATASET_ROOT>/pipelines directory"
        )
    )
    parser.add_argument("dataset", type=Path)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the changes without applying them.",
    )
    return parser


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()

    migrate_dataset(args.dataset, args.dry_run)
