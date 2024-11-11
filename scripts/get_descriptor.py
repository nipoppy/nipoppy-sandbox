#!/usr/bin/env python
import argparse
from importlib import import_module
from pathlib import Path
import boutiques

NAME_FIELD = 'name'
VERSION_FIELD = 'tool-version'
COMMAND_LINE_FIELD = 'command-line'
INPUTS_FIELD = 'inputs'
DEFAULT_VALUE_FIELD = 'default-value'
ID_FIELD = 'id'
CHOICES_FIELD = 'value-choices'
KEY_FIELD = 'value-key'
DESCRIPTION_FIELD = 'description'

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description=(
        'Generate a Boutiques descriptor for a given tool.'
        '\n- Only works for CLIs built with argparse.'
        '\n- Need to make sure to checkout the right version: git checkout tags/<TAG>.'
        '\n  Then do pip install'
        '\n- This script needs to be modified for new tools.'
        '\n- The descriptor might need some manual tweaking too.'
        ),
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument('tool_name', type=str, help='Name of the tool to generate the descriptor for.')
    parser.add_argument('--module-name', type=str, help='Name of the module to import (default: same as tool name).')
    parser.add_argument('--tool-version', type=str, help='Version of the tool to use (default: obtained from the package).')

    args = parser.parse_args()

    tool_name = args.tool_name
    module_name = args.module_name
    tool_version = args.tool_version

    if module_name is None:
        module_name = tool_name

    try:
        module = import_module(module_name)
        if tool_name in ['fmriprep', 'mriqc']:
            path_parser_module = module_name
            for submodule_name in ('cli', 'parser'):
                path_parser_module = f'{path_parser_module}.{submodule_name}'
                parser_module = import_module(path_parser_module)
            parser = parser_module._build_parser()
        elif tool_name in ['halfpipe']:
            path_parser_module = f'{module_name}.cli.parser'
            parser_module = import_module(path_parser_module)
            parser = parser_module.build_parser()
        elif tool_name in ['heudiconv']:
            path_parser_module = f'{module_name}.cli.run'
            parser_module = import_module(path_parser_module)
            parser = parser_module.get_parser()
        elif tool_name in ['dcm2bids', 'dcm2bids_helper']:
            path_parser_module = f'{module_name}.cli.{tool_name}'
            parser_module = import_module(path_parser_module)
            parser = parser_module._build_arg_parser()
        elif tool_name in ['bidsmapper', 'bidseditor', 'bidscoiner']:
            path_parser_module = f'{module_name}.cli._{tool_name}'
            parser_module = import_module(path_parser_module)
            parser = parser_module.get_parser()
        elif tool_name in ['qsiprep']:
            # older versions
            try:
                path_parser_module = f'{module_name}.cli.run'
                parser_module = import_module(path_parser_module)
                parser = parser_module.get_parser()
            # newer versions
            except AttributeError:
                path_parser_module = f'{module_name}.cli.parser'
                parser_module = import_module(path_parser_module)
                parser = parser_module._build_parser()
        else:
            raise RuntimeError(f'Unable to get parser for {tool_name}. Check this script and make necessary changes.')
    except Exception as exception:
        print(
            f'Error while importing modules dynamically. Make sure that {module_name} has been installed '
            'in this environment and that the import for the parser is correct.'
        )
        raise exception

    if tool_version is None:
        try:
            tool_version = module.__version__
        except AttributeError:
            tool_version = import_module(f'{module_name}.version').__version__

    fpath_out = f'{tool_name}-{tool_version}.json'

    print(f'tool_version: {tool_version}')

    new_descriptor = boutiques.creator.CreateDescriptor(
        parser,
        execname=tool_name,
    )

    new_descriptor.descriptor[NAME_FIELD] = tool_name
    new_descriptor.descriptor[VERSION_FIELD] = tool_version
    new_descriptor.descriptor[DESCRIPTION_FIELD] = tool_name

    # print(new_descriptor.descriptor)
    for input_object in new_descriptor.descriptor[INPUTS_FIELD]:
        if (DEFAULT_VALUE_FIELD in input_object) and isinstance(input_object[DEFAULT_VALUE_FIELD], Path):
            print(input_object)

    # fix errors
    FIELDS_TO_CHECK = ["work_dir", "output_dir", "template"]
    TO_RENAME_BY_PIPELINE = {'fmriprep': [('run_reconall','skip_reconall')]}
    for input_object in new_descriptor.descriptor[INPUTS_FIELD]:

        # delete default values that are pathlib.Path objects
        for field_to_check in FIELDS_TO_CHECK:
            if input_object[ID_FIELD] == field_to_check and isinstance(input_object.get(DEFAULT_VALUE_FIELD), Path):
                print(f"Deleting default value for {field_to_check}: {input_object[DEFAULT_VALUE_FIELD]}")
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
                    new_descriptor.descriptor[COMMAND_LINE_FIELD] = new_descriptor.descriptor[COMMAND_LINE_FIELD].replace(old_key, new_key)

        
        # delete default values that are ==SUPPRESS==
        if input_object.get(DEFAULT_VALUE_FIELD) == "==SUPPRESS==":
            print(f"Deleting default value for {input_object[ID_FIELD]}: {input_object[DEFAULT_VALUE_FIELD]}")
            del input_object[DEFAULT_VALUE_FIELD]

        # remove choices that are None
        try:
            tmp = input_object[CHOICES_FIELD]
            input_object[CHOICES_FIELD] = [choice for choice in input_object[CHOICES_FIELD] if choice is not None]
            if len(tmp) != len(input_object[CHOICES_FIELD]):
                print(f"Removed None choice for {input_object[ID_FIELD]}: {tmp} -> {input_object[CHOICES_FIELD]}")
        except KeyError:
            pass

        # add a dummy description if none is provided
        if input_object[DESCRIPTION_FIELD] is None:
            input_object[DESCRIPTION_FIELD] = 'No description provided.'

    print('===== Saving =====')
    new_descriptor.save(fpath_out)
    print(fpath_out)

    print('===== Validating ===== ')
    print(boutiques.bosh(['validate', fpath_out]))
