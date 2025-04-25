#!/usr/bin/env python
import argparse
from importlib import import_module
from pathlib import Path
import boutiques

NAME_FIELD = "name"
VERSION_FIELD = "tool-version"
COMMAND_LINE_FIELD = "command-line"
INPUTS_FIELD = "inputs"
DEFAULT_VALUE_FIELD = "default-value"
ID_FIELD = "id"
CHOICES_FIELD = "value-choices"
KEY_FIELD = "value-key"
DESCRIPTION_FIELD = "description"
TYPE_FIELD = "type"
FLAG_FIELD = "command-line-flag"
LIST_FIELD = "list"

TYPE_NUMBER = "Number"
TYPE_FLAG = "Flag"
TYPE_STRING = "String"

def process_descriptor(parser, tool_name, tool_version):
    new_descriptor = boutiques.creator.CreateDescriptor(
        parser,
        execname=tool_name,
    )

    new_descriptor.descriptor[NAME_FIELD] = tool_name
    new_descriptor.descriptor[VERSION_FIELD] = tool_version
    new_descriptor.descriptor[DESCRIPTION_FIELD] = tool_name

    # print(new_descriptor.descriptor)

    # fix errors
    FIELDS_TO_CHECK = ["work_dir", "output_dir", "template"]
    TO_RENAME_BY_PIPELINE = {
        "fmriprep": [
            ("memory_gb", "mem"),
            ("use_bbr", "force_bbr"),
            ("run_reconall", "fs_no_reconall"),
            ("run_msmsulc", "no_msm"),
            ("hires", "no_submm_recon"),
            ("regressors_all_comps", "return_all_components"),
        ],
        "qsiprep": [("bids_filters", "bids_filter_file"), ("memory_gb", "mem")],
        "mriqc": [("memory_gb", "mem")],
        "dcm2bids_helper": [("overwrite", "force")],
        "xcp_d": [
            ("bids_filters", "bids_filter_file"),
            ("memory_gb", "mem_gb"),
            ("bandpass_filter", "disable_bandpass_filter"),
            ("process_surfaces", "warp_surfaces_native2std"),
            ("dcan_qc", "skip_dcan_qc")
        ] 
    }
    I_INPUT_BBR = None  # may need to be deleted/replaced for fMRIPrep
    for i_input, input_object in enumerate(new_descriptor.descriptor[INPUTS_FIELD]):

        # delete default values that are pathlib.Path objects
        for field_to_check in FIELDS_TO_CHECK:
            if input_object[ID_FIELD] == field_to_check and isinstance(
                input_object.get(DEFAULT_VALUE_FIELD), Path
            ):
                print(
                    f"Deleting default value for {field_to_check} ({input_object[DEFAULT_VALUE_FIELD]})"
                )
                del input_object[DEFAULT_VALUE_FIELD]

        # rename some inputs
        if tool_name in TO_RENAME_BY_PIPELINE:
            for old_id, new_id in TO_RENAME_BY_PIPELINE[tool_name]:
                if input_object[ID_FIELD] == old_id:
                    print(f"Renaming {old_id} -> {new_id}")
                    input_object[ID_FIELD] = new_id
                    input_object[NAME_FIELD] = new_id
                    old_key = str(input_object[KEY_FIELD])
                    new_key = old_key.lower().replace(old_id, new_id).upper()
                    input_object[KEY_FIELD] = new_key
                    new_descriptor.descriptor[COMMAND_LINE_FIELD] = (
                        new_descriptor.descriptor[COMMAND_LINE_FIELD].replace(
                            old_key, new_key
                        )
                    )

        # delete default values that are ==SUPPRESS==
        if input_object.get(DEFAULT_VALUE_FIELD) == "==SUPPRESS==":
            print(
                f"Deleting default value for {input_object[ID_FIELD]} ({input_object[DEFAULT_VALUE_FIELD]})"
            )
            del input_object[DEFAULT_VALUE_FIELD]

        # remove choices that are None
        try:
            tmp = input_object[CHOICES_FIELD]
            input_object[CHOICES_FIELD] = [
                choice for choice in input_object[CHOICES_FIELD] if choice is not None
            ]
            if len(tmp) != len(input_object[CHOICES_FIELD]):
                print(
                    f"Removed None choice for {input_object[ID_FIELD]}: {tmp} -> {input_object[CHOICES_FIELD]}"
                )
        except KeyError:
            pass

        # add a dummy description if none is provided
        if input_object[DESCRIPTION_FIELD] is None:
            input_object[DESCRIPTION_FIELD] = "No description provided."

        # tool-specific fixes
        if tool_name == "fmriprep":
            # lists
            if input_object[ID_FIELD] in ["output_spaces"]:
                input_object[LIST_FIELD] = True
                print(
                    f'Setting "{LIST_FIELD}" field to True for {input_object[ID_FIELD]}'
                )
            # type
            if input_object[ID_FIELD] in ["aggr_ses_reports"]:
                input_object[TYPE_FIELD] = TYPE_NUMBER
                print(
                    f'Setting "{TYPE_FIELD}" field to {TYPE_NUMBER} for {input_object[ID_FIELD]}'
                )
            if input_object[ID_FIELD] in [
                "no_msm",
                "use_aroma",
                "aroma_err_on_warn",
                "fmap_no_demean",
                "no_submm_recon",
                "fs_no_reconall",
                "version",
            ]:
                input_object[TYPE_FIELD] = TYPE_FLAG
                print(
                    f'Setting "{TYPE_FIELD}" field to {TYPE_FLAG} for {input_object[ID_FIELD]}'
                )
                if DEFAULT_VALUE_FIELD in input_object:
                    print(
                        f"Deleting default value for {input_object[ID_FIELD]} ({input_object[DEFAULT_VALUE_FIELD]})"
                    )
                    del input_object[DEFAULT_VALUE_FIELD]
            # fMRIPrep CLI has --force-bbr and --force-no-bbr flags
            # they are both configured to have dest='use_bbr', and the Boutiques
            # descriptor builder only keeps the first one
            if input_object[ID_FIELD] == "force_bbr":
                I_INPUT_BBR = i_input

        elif tool_name == "xcp_d":
            if input_object[ID_FIELD] in [
                "disable_bandpass_filter",
                "skip_dcan_qc"
            ]:
                input_object[TYPE_FIELD] = TYPE_FLAG
                print(
                    f'Setting "{TYPE_FIELD}" field to {TYPE_FLAG} for {input_object[ID_FIELD]}'
                )
                if DEFAULT_VALUE_FIELD in input_object:
                    print(
                        f"Deleting default value for {input_object[ID_FIELD]} ({input_object[DEFAULT_VALUE_FIELD]})"
                    )
                    del input_object[DEFAULT_VALUE_FIELD]
            # type
            if input_object[ID_FIELD] in ["head_radius", "min_coverage"]:
                input_object[TYPE_FIELD] = TYPE_NUMBER
                print(
                    f'Setting "{TYPE_FIELD}" field to {TYPE_NUMBER} for {input_object[ID_FIELD]}'
                )

        elif tool_name == "mriqc":
            if input_object[ID_FIELD] in ["version"]:
                input_object[TYPE_FIELD] = TYPE_FLAG
                print(
                    f'Setting "type" field to {TYPE_FLAG} for {input_object[ID_FIELD]}'
                )
                if DEFAULT_VALUE_FIELD in input_object:
                    print(
                        f"Deleting default value for {input_object[ID_FIELD]} ({input_object[DEFAULT_VALUE_FIELD]})"
                    )
                    del input_object[DEFAULT_VALUE_FIELD]
            if input_object[ID_FIELD] == "analysis_level":
                if LIST_FIELD in input_object:
                    del input_object[LIST_FIELD]
                    print(f'Deleting "{LIST_FIELD}" field for {input_object[ID_FIELD]}')
            if input_object[ID_FIELD] == "modalities":
                input_object[LIST_FIELD] = True
                print(
                    f'Setting "{LIST_FIELD}" field to True for {input_object[ID_FIELD]}'
                )
        elif tool_name == "qsiprep":
            # wrong type
            if input_object[ID_FIELD] in ["nprocs", "omp_nthreads"]:
                input_object[TYPE_FIELD] = TYPE_NUMBER
            # flags
            if input_object[ID_FIELD] in ["fmap_no_demean", "version", "longitudinal"]:
                input_object[TYPE_FIELD] = TYPE_FLAG
                print(
                    f'Setting "type" field to {TYPE_FLAG} for {input_object[ID_FIELD]}'
                )
                if DEFAULT_VALUE_FIELD in input_object:
                    print(
                        f"Deleting default value for {input_object[ID_FIELD]} ({input_object[DEFAULT_VALUE_FIELD]})"
                    )
                    del input_object[DEFAULT_VALUE_FIELD]
        elif tool_name == "heudiconv":
            if input_object[ID_FIELD] == "bids_options":
                input_object[LIST_FIELD] = True
                print(
                    f'Setting "{LIST_FIELD}" field to True for {input_object[ID_FIELD]}'
                )

        # Nipreps tools 'verbose_count' input
        if tool_name in ["fmriprep", "mriqc", "qsiprep", "xcp_d"]:
            if input_object[ID_FIELD] == "verbose_count":
                input_object[CHOICES_FIELD] = ["-v", "-vv", "-vvv"]
                print(f'Setting "choices" field for {input_object[ID_FIELD]}')
                if FLAG_FIELD in input_object:
                    print(
                        f"Deleting flag for {input_object[ID_FIELD]} ({input_object[FLAG_FIELD]})"
                    )
                    del input_object[FLAG_FIELD]

        if isinstance(input_object.get(DEFAULT_VALUE_FIELD), Path):
            print(
                f"WARNING: pathlib.Path default value for {input_object[ID_FIELD]}: {input_object[DEFAULT_VALUE_FIELD]}"
            )

        if (
            input_object[TYPE_FIELD] == TYPE_STRING
            and input_object.get(DEFAULT_VALUE_FIELD) is True
        ):
            print(
                f"WARNING: String with default value True, should check: {input_object[ID_FIELD]}"
            )

        if (
            input_object[TYPE_FIELD] == TYPE_FLAG
            and input_object.get(DEFAULT_VALUE_FIELD) is True
        ):
            print(
                f"WARNING: Flag with default value True, should check: {input_object[ID_FIELD]}"
            )

        if FLAG_FIELD in input_object:
            snake_case_flag = input_object[FLAG_FIELD].lstrip("-").replace("-", "_")
            if snake_case_flag != input_object[ID_FIELD]:
                print(
                    f"WARNING: ID and flag do not match, rename {input_object[ID_FIELD]} -> {snake_case_flag}?"
                )

    if I_INPUT_BBR is not None:
        print("Adding entry for --force-no-bbr")
        # sanity check that the existing entry is what we think it is
        if (
            not new_descriptor.descriptor[INPUTS_FIELD][I_INPUT_BBR][FLAG_FIELD]
            == "--force-bbr"
        ):
            print(
                'WARNING: double-check the input item for "force_bbr" (unexpected flag)'
            )
        # add missed entry right after the existing one
        existing_key = new_descriptor.descriptor[INPUTS_FIELD][I_INPUT_BBR][KEY_FIELD]
        new_key = "[FORCE_NO_BBR]"
        new_descriptor.descriptor[INPUTS_FIELD].insert(
            I_INPUT_BBR + 1,
            {
                ID_FIELD: "force_no_bbr",
                NAME_FIELD: "force_no_bbr",
                DESCRIPTION_FIELD: "Do not use boundary-based registration (no goodness-of-fit checks)",
                TYPE_FIELD: TYPE_FLAG,
                FLAG_FIELD: "--force-no-bbr",
                "optional": True,
                KEY_FIELD: new_key,
            },
        )
        new_descriptor.descriptor[COMMAND_LINE_FIELD] = new_descriptor.descriptor[
            COMMAND_LINE_FIELD
        ].replace(existing_key, f"{existing_key} {new_key}")

    return new_descriptor

def get_descriptor(tool_name, module_name, tool_version):

    if module_name is None:
        module_name = tool_name

    try:
        module = import_module(module_name)
        if tool_name in ["fmriprep", "mriqc", "xcp_d"]:
            path_parser_module = module_name
            for submodule_name in ("cli", "parser"):
                path_parser_module = f"{path_parser_module}.{submodule_name}"
                parser_module = import_module(path_parser_module)
            parser = parser_module._build_parser()
        elif tool_name in ["halfpipe"]:
            path_parser_module = f"{module_name}.cli.parser"
            parser_module = import_module(path_parser_module)
            parser = parser_module.build_parser()
        elif tool_name in ["heudiconv"]:
            path_parser_module = f"{module_name}.cli.run"
            parser_module = import_module(path_parser_module)
            parser = parser_module.get_parser()
        elif tool_name in ["dcm2bids", "dcm2bids_helper"]:
            path_parser_module = f"{module_name}.cli.{tool_name}"
            parser_module = import_module(path_parser_module)
            parser = parser_module._build_arg_parser()
        elif tool_name in ["bidsmapper", "bidseditor", "bidscoiner"]:
            path_parser_module = f"{module_name}.cli._{tool_name}"
            parser_module = import_module(path_parser_module)
            parser = parser_module.get_parser()
        elif tool_name in ["qsiprep"]:
            # older versions
            try:
                path_parser_module = f"{module_name}.cli.run"
                parser_module = import_module(path_parser_module)
                parser = parser_module.get_parser()
            # newer versions
            except AttributeError:
                path_parser_module = f"{module_name}.cli.parser"
                parser_module = import_module(path_parser_module)
                parser = parser_module._build_parser()
        else:
            raise RuntimeError(
                f"Unable to get parser for {tool_name}. Check this script and make necessary changes."
            )
    except Exception as exception:
        print(
            f"Error while importing modules dynamically. Make sure that {module_name} has been installed "
            "in this environment and that the import for the parser is correct."
        )
        raise exception

    if tool_version is None:
        try:
            tool_version = module.__version__
        except AttributeError:
            tool_version = import_module(f"{module_name}.version").__version__

    fpath_out = f"{tool_name}-{tool_version}.json"

    print(f"tool_version: {tool_version}")

    new_descriptor = process_descriptor(parser, tool_name, tool_version)

    print("===== Saving =====")
    new_descriptor.save(fpath_out)
    print(fpath_out)

    print("===== Validating ===== ")
    print(boutiques.bosh(["validate", fpath_out]))

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=(
            "Generate a Boutiques descriptor for a given tool."
            "\n- Only works for CLIs built with argparse."
            "\n- Need to make sure to checkout the right version: git checkout tags/<TAG>."
            "\n  Then do pip install"
            "\n- This script needs to be modified for new tools."
            "\n- The descriptor might need some manual tweaking too."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "tool_name", type=str, help="Name of the tool to generate the descriptor for."
    )
    parser.add_argument(
        "--module-name",
        type=str,
        help="Name of the module to import (default: same as tool name).",
    )
    parser.add_argument(
        "--tool-version",
        type=str,
        help="Version of the tool to use (default: obtained from the package).",
    )

    args = parser.parse_args()

    tool_name = args.tool_name
    module_name = args.module_name
    tool_version = args.tool_version

    get_descriptor(tool_name, module_name, tool_version)