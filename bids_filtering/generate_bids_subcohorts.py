import numpy as np
import glob
import pandas as pd
import os 
import argparse
import bids2table as b2t2
import json

# default table is too wide, keep only relevant columns
PARSE_TAGS = ['dataset', 'sub', 'ses', 'acq', 'dir', 'run', 'echo',  'desc', 'datatype', 'suffix', 'ext', 'extra_entities', 'path']
        

def get_bids_df(ds_path, bids_table_index_path, read_bids_df=False, save_bids_df=False):
    """
    Get bids dataframe from bids2table or read from csv if already saved.
    """
    if read_bids_df:
        # read dataframe from csv
        print(f"Reading bids_df from {bids_table_index_path}")
        bids_df = pd.read_csv(bids_table_index_path, sep='\t')

    else:
        # index with b2t2
        print(f"Indexing BIDS dataset from {ds_path}...")
        tab = b2t2.index_dataset(ds_path)
        bids_df = tab.to_pandas(types_mapper=pd.ArrowDtype)
        bids_df = bids_df[PARSE_TAGS]

        # save dataframe to csv
        if save_bids_df:        
            bids_df.to_csv(bids_table_index_path, index=False, sep='\t')
            print(f"Dataframe saved to {bids_table_index_path}")

    return bids_df

def get_scanner_metadata(ds_path, bids_df, bids_table_metadata_path, scanner_metadata, read_metadata_df=False, save_metadata_df=False):
    """
    Extract scanner metadata from JSON sidecar files.
    """

    if read_metadata_df:
        # read dataframe from csv
        print(f"Reading metadata_df from {bids_table_metadata_path}")
        metadata_df = pd.read_csv(bids_table_metadata_path, sep='\t')

    else:
        # get single T1w json files from bids_df per participant (bids to table does not list jsons...)
        t1w_nii_files = bids_df[(bids_df['datatype'] == 'anat') & (bids_df['suffix'] == 'T1w') & (bids_df['ext'] == '.nii.gz')]['path'].unique()
        print(f"Number of T1w json files: {len(t1w_nii_files)}")
        t1w_json_files = [f.replace('.nii.gz', '.json') for f in t1w_nii_files]

        metadata_list = []
        for json_file in t1w_json_files:
            json_file_path = f"{ds_path}/{json_file}"
            with open(json_file_path, 'r') as f:
                json_data = json.load(f)
            
            metadata = {}
            sidecar_tags = scanner_metadata.get("sidecar_tags", [])
            if sidecar_tags:
                for tag in sidecar_tags:
                    metadata[tag] = json_data.get(tag, None)
           
            # get sub and ses from path
            parts = json_file.split(os.sep)
            sub = [part.split("-")[1] for part in parts if part.startswith('sub-')]
            ses = [part.split("-")[1] for part in parts if part.startswith('ses-')]
            acq = [part.split("-")[1] for part in parts if part.startswith('acq-')]

            metadata['sub'] = sub[0] if sub else None
            metadata['ses'] = ses[0] if ses else None
            metadata['acq'] = acq[0] if acq else None
            
            metadata_list.append(metadata)

        metadata_df = pd.DataFrame(metadata_list)

        if save_metadata_df:
            metadata_df.to_csv(bids_table_metadata_path, index=False, sep='\t')
            print(f"Metadata dataframe saved to {bids_table_metadata_path}")

    return metadata_df

    

def create_count_table(bids_df, groupby_cols, count_cols, save_table_path=None):
    """
    Create a table with counts of a specific column grouped by specified columns.
    """
    print(f"Creating count table grouped by {groupby_cols} counting unique {count_cols}...")
    count_df = bids_df.groupby(groupby_cols)[count_cols].nunique().reset_index()    
    # rename count columns
    rename_dict = {col: f"n_{col}s" for col in count_cols}
    
    count_df = count_df.rename(columns=rename_dict)

    if save_table_path:
        count_df.to_csv(save_table_path, index=False, sep='\t')
        print(f"Count table saved to {save_table_path}")

    return count_df

def filter_by_datatype(bids_df, datatypes):
    """
    Filter dataframe based on specified datatypes.
    """
    filtered_df = bids_df[bids_df['datatype'].isin(datatypes)]
    participants_with_all_datatypes = filtered_df.groupby('sub')['datatype'].nunique()
    participants_with_all_datatypes = participants_with_all_datatypes[participants_with_all_datatypes == len(datatypes)].index

    filtered_df = filtered_df[filtered_df['sub'].isin(participants_with_all_datatypes)]
    return filtered_df

def filter_by_metadata(metadata_df, metadata_criteria):
    """
    Filter dataframe based on metadata criteria.
    """
    participants_with_metadata = metadata_df['sub'].unique()
    for tag, value in metadata_criteria.items():
        print(f"Filtering by metadata tag: {tag} with value: {value}")
        participants_with_metadata = set(participants_with_metadata).intersection(set(metadata_df[metadata_df[tag].isin(value)]['sub']))

    filtered_df = metadata_df[metadata_df['sub'].isin(participants_with_metadata)]
    return filtered_df

def filter_by_protocol_counts(count_df, protocol_spec, force_exact_counts=False, save_table_path=None):
    """
    Filter participants based on criteria dictionary.
    """

    # filter based on protocol specifications
    criteria = []
    for crit in protocol_spec:
        mask = np.array([True]*len(count_df))
        for tag, value in crit.items():    
            if force_exact_counts:
                mask &= (count_df[tag] == value) # exact match
            else:
                mask &= (count_df[tag] >= value) # greater than or equal to match
                
        criteria.append(mask.copy())

    # check if all criteria are met for each participant and session
    if len(criteria) > 0:
        combined_criteria = np.logical_or.reduce(criteria)
        count_df['criteria_met'] = combined_criteria
    else:
        count_df['criteria_met'] = False
    
    count_df_filtered = count_df[count_df['criteria_met']].groupby(['ses'])["sub"].unique().reset_index(name='participants')

    # Save filtered df
    if save_table_path:
        pd.DataFrame(count_df_filtered).to_csv(save_table_path, index=False, sep='\t')
    
    return count_df_filtered


def save_participant_lists(count_df_filtered, criteria_name, output_dir):
    """
    Save participant lists to text files based on filtered count dataframe.
    """
    if len(count_df_filtered) == 0:
        print(f"No participants found matching criteria: {criteria_name}")
        return

    os.makedirs(f"{output_dir}/{criteria_name}", exist_ok=True)
    sessions = count_df_filtered['ses'].tolist()
    
    for ses in sessions:
        participants = count_df_filtered[count_df_filtered['ses'] == ses]['participants'].values[0]
        participant_list_path = f"{output_dir}/{criteria_name}/participants_{ses}.txt"
        
        with open(participant_list_path, 'w') as f:
            for p in participants:
                f.write(f"{p}\n")

    print(f"Participant list for each session saved to {output_dir}/{criteria_name}")


def run(nipoppy_ds_path, read_bids_df, read_metadata_df, bids_filter_spec_file, bids_filter_spec_name, output_dir):
    """
    Main function to run the filtering process.
    """
    bid_ds_path = f"{nipoppy_ds_path}/bids/"
    os.makedirs(output_dir, exist_ok=True)

    # paths for intermediate files
    bids_table_index_path = f"{output_dir}/bids2table_index.tsv"
    bids_table_metadata_path = f"{output_dir}/bids2table_metadata.tsv"
    single_shell_table_path = f"{output_dir}/single_shell_table.tsv"
    multi_shell_table_path = f"{output_dir}/multi_shell_table.tsv"

    # only save if not reading from preexisting files
    save_bids_df = not read_bids_df
    save_metadata_df = not read_metadata_df
    print(f"Reading preexisting bids_df: {read_bids_df}, saving bids_df: {save_bids_df}")
    print(f"Reading preexisting metadata_df: {read_metadata_df}, saving metadata_df: {save_metadata_df}")

    # create bids index table
    bids_df = get_bids_df(bid_ds_path, bids_table_index_path, read_bids_df=read_bids_df, save_bids_df=save_bids_df)

    # number of participants
    n_subs = len(bids_df['sub'].unique())
    print(f"Number of participants: {n_subs}")    

    # Per session bids participant counts
    availability_df = bids_df.groupby(['ses'])['sub'].nunique().reset_index(name='total_participants')

    # read filter specifications
    with open(bids_filter_spec_file, 'r') as f:
        filter_spec_dict = json.load(f)  

    # select filter spec
    filter_spec = filter_spec_dict[bids_filter_spec_name]

    os.makedirs(f"{output_dir}/{bids_filter_spec_name}", exist_ok=True)

    # create scanner metadata table
    scanner_metadata = filter_spec["scanner_metadata"]
    metadata_df = get_scanner_metadata(bid_ds_path, bids_df, bids_table_metadata_path, scanner_metadata, read_metadata_df=read_metadata_df, save_metadata_df=save_metadata_df)

    # Start filtering
    # check extra options
    extra_options = filter_spec["criteria"]["extra_options"]
    force_exact_counts = extra_options.get("force_exact_counts", False)

    # filter metadata_df by scanner metadata
    metadata_criteria = filter_spec["criteria"]["scanner_metadata"]["sidecar_tags"]
    filter_df = filter_by_metadata(metadata_df, metadata_criteria)
    metadata_participants = filter_df['sub'].unique()
    
    metadata_availability_df = filter_df.groupby(['ses'])['sub'].nunique().reset_index(name='metadata_participants')
    availability_df = availability_df.merge(metadata_availability_df, on='ses', how='left')

    # filter bids_df by datatypes
    datatypes = filter_spec["criteria"]["datatypes"]
    filter_df = filter_by_datatype(bids_df, datatypes)
    datatype_participants = filter_df['sub'].unique()

    datatype_availability_df = filter_df.groupby(['ses'])['sub'].nunique().reset_index(name='datatype_participants')
    availability_df = availability_df.merge(datatype_availability_df, on='ses', how='left')
   
    # Broader filter prior to count table based on datatype specific protocol counts
    count_participants = set(datatype_participants).intersection(set(metadata_participants))
    bids_df = bids_df[bids_df['sub'].isin(count_participants)]

    # create count table based groupby and count columns (this is meant for specific datatype protocol counts)
    save_table_path = f"{output_dir}/{bids_filter_spec_name}/count_table.tsv"
    count_df = create_count_table(
        bids_df,
        groupby_cols=filter_spec["groupby_cols"],
        count_cols=filter_spec["count_cols"],
        save_table_path=save_table_path
    )

    # get filter criteria
    protocol_spec = filter_spec["criteria"]["protocol_spec"]

    # apply criteria to filter participants
    save_table_path = f"{output_dir}/{bids_filter_spec_name}/filtered_participants.tsv"
    filtered_df = filter_by_protocol_counts(count_df, protocol_spec, force_exact_counts, save_table_path=save_table_path)
    filtered_df[bids_filter_spec_name] = filtered_df['participants'].apply(len)

    # availability table with participant counts
    availability_df = availability_df.merge(filtered_df[['ses', bids_filter_spec_name]], on='ses', how='left')
    availability_df = availability_df.set_index('ses').fillna(0).astype(int)
    print(availability_df)

    # TODO add phase encoding direction criteria

    save_participant_lists(filtered_df, bids_filter_spec_name, output_dir)

# main
if __name__ == "__main__":
    
    # argeparse
    parser = argparse.ArgumentParser(description="Filter BIDS participants based on criteria.")
    parser.add_argument('--ds_path', type=str, default=None, help='Path to the Nipoppy dataset.')
    parser.add_argument('--read_bids_df', action='store_true', help='Read bids_df from previously saved file.')
    parser.add_argument('--read_metadata_df', action='store_true', help='Read metadata_df from previously saved file.')
    parser.add_argument('--bids_filter_spec_file', type=str, default=None, help='Path to the bids filter specification JSON file.')
    parser.add_argument('--bids_filter_spec_name', type=str, help='filter name from the specification file.')
    parser.add_argument('--output_dir', type=str, default=None, help='Path to save participant lists and tables.')
    args = parser.parse_args()

    ds_path = args.ds_path
    read_bids_df = args.read_bids_df
    read_metadata_df = args.read_metadata_df
    bids_filter_spec_file = args.bids_filter_spec_file
    bids_filter_spec_name = args.bids_filter_spec_name
    output_dir = args.output_dir

    run(ds_path, read_bids_df, read_metadata_df, bids_filter_spec_file, bids_filter_spec_name, output_dir)

    
