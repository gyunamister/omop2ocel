from utils import convert_to_datetime
import pandas as pd

def process_dataframes(dfs, transformations, key_columns):
    """Process all dataframes to convert dates and select specific columns."""
    processed_dfs = {}
    for name, transformation in zip(dfs.keys(), transformations):
        # Validate transformation structure
        if not isinstance(transformation, tuple) or len(transformation) != 2:
            processed_df = convert_to_datetime(dfs[name], ["visit_start_datetime", "visit_end_datetime"])
            processed_dfs[name] = processed_df[key_columns]
            continue
        cols, additional_cols = transformation
        date_columns = ["visit_start_datetime", "visit_end_datetime"] + (cols if cols is not None else [])
        processed_df = convert_to_datetime(dfs[name], date_columns)
        processed_dfs[name] = processed_df[key_columns + additional_cols]
    return processed_dfs

def join_vocabulary_data(df, voca_df, class_df, concept_id_col):
    """Enrich a dataframe with concept and concept class data."""
    df = pd.merge(df, voca_df, left_on=concept_id_col, right_on='concept_id', how='left')
    df = pd.merge(df, class_df, on='concept_class_id', how='left')
    return df

def device_mapping(df1, device_df):
    """Merges dataframes with device data, filters, groups, and pivots the data."""
    merged_df = pd.merge(df1, device_df, on=['visit_occurrence_id', 'person_id'])
    filtered_df = merged_df[(merged_df['visit_start_datetime_x'] <= merged_df['device_exposure_start_datetime']) & (merged_df['visit_end_datetime_x'] >= merged_df['device_exposure_end_datetime'])]
    final_df = filtered_df.groupby(['visit_occurrence_id', 'person_id', 'concept_class_name_y']).agg({'unique_device_id': 'first'}).reset_index()
    pivot_df = final_df.pivot_table(index=['visit_occurrence_id', 'person_id'], columns='concept_class_name_y', values='unique_device_id', aggfunc='first')
    return df1.join(pivot_df, on=['visit_occurrence_id', 'person_id'])

def create_ocel_event(source_df, activity_type, timestamp_cols, common_cols, provider_cols=None, device_cols=None, is_visit_event=False):
    ocel_df = pd.DataFrame()

    # Handling timestamp columns
    for dest_col, src_col in timestamp_cols.items():
        ocel_df[dest_col] = source_df[src_col]

    # Handling static and activity columns
    for col, value in common_cols['static_cols'].items():
        ocel_df[col] = source_df[value]

    # Special handling for visit_start and visit_end
    if is_visit_event:
        ocel_df['activity_ML'] = [activity_type] * len(source_df)
        ocel_df['activity_LL'] = [activity_type] * len(source_df)
    else:
        for col, value in common_cols['activity_cols'].items():
            ocel_df[col] = source_df[value]

    # Conditionally add device columns if included
    if device_cols:
        for col, value in device_cols.items():
            if value in source_df.columns:
                ocel_df[col] = source_df[value]

    # Set high-level activity label
    ocel_df['activity_HL'] = [activity_type] * len(source_df)
    return ocel_df

def preprocess_ocel_data(FinalOCEL):
    # Convert timestamps to datetime format for the entire DataFrame
    FinalOCEL["timestamp"] = pd.to_datetime(FinalOCEL["timestamp"], format="%Y-%m-%d %H:%M:%S")
    FinalOCEL["event_start_timestamp"] = pd.to_datetime(FinalOCEL["event_start_timestamp"], format="%Y-%m-%d %H:%M:%S")
    FinalOCEL["event_end_timestamp"] = pd.to_datetime(FinalOCEL["event_end_timestamp"], format="%Y-%m-%d %H:%M:%S")

    # Initialize list for indices to drop and list of valid visits
    indices_to_drop = []
    valid_visits = []

    # Group by 'visit' to handle each group
    grouped = FinalOCEL.groupby('visit')
    for visit, group in grouped:
        if 'visit_start' in group['activity_HL'].values and 'visit_end' in group['activity_HL'].values:
            start = group[group['activity_HL'] == 'visit_start']['timestamp'].iloc[0]
            end = group[group['activity_HL'] == 'visit_end']['timestamp'].iloc[0]

            if start < end:
                valid_visits.append(visit)
                out_of_bounds = group[(group['event_start_timestamp'] < start) | (group['event_end_timestamp'] > end)].index
                indices_to_drop.extend(out_of_bounds)

    # Filter to include only valid visits
    FinalOCEL = FinalOCEL[FinalOCEL['visit'].isin(valid_visits)]

    # Drop the out-of-bounds events
    FinalOCEL.drop(indices_to_drop, inplace=True)

    # Remove rows where 'activity_LL' is 'No matching concept'
    OCEL_df_prep_unmatched = FinalOCEL[FinalOCEL['activity_LL'] != 'No matching concept']

    # Drop duplicates based on specific columns
    OCEL_df_prep_unmatched.drop_duplicates(subset=['timestamp', 'visit', 'activity_HL', 'activity_LL'], ignore_index=True, inplace=True)

    # Optionally capture the DataFrame after duplicates are removed
    OCEL_df_prep_duplicates = OCEL_df_prep_unmatched

    return OCEL_df_prep_duplicates

def sort_ocel_data(df):
    # Assign artificial order based on activity type
    activity_order = {
        'visit_start': 0,
        'observation': 1,
        'measurement': 2,
        'condition': 3,
        'procedure': 4,
        'drug': 5,
        'visit_end': 6
    }

    # Map activities to their respective order
    df['artificial_order'] = df['activity_HL'].map(activity_order)

    # Convert timestamp to datetime format if not already
    if df['timestamp'].dtype != '<M8[ns]':  # '<M8[ns]' is numpy's datetime64 type
        df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Sort the DataFrame
    df.sort_values(by=['visit', 'timestamp', 'artificial_order', 'activity_ML', 'activity_LL'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Drop the artificial order column used for sorting
    df.drop('artificial_order', axis=1, inplace=True)

    return df