def abstract_events(df, activity_column):
    # Mapping activities to their respective order
    activity_order = {
        'visit_start': 0,
        'observation': 1,
        'measurement': 2,
        'condition': 3,
        'procedure': 4,
        'drug': 5,
        'visit_end': 6
    }

    # Apply mapping to the DataFrame
    df['artificial_order'] = df['activity_HL'].map(activity_order)

    # Sort the DataFrame by the specified columns
    df.sort_values(by=['visit', 'timestamp', 'artificial_order', 'activity_ML', 'activity_LL'], inplace=True)

    # Remove sequential identical events more efficiently
    df['prev_activity'] = df[activity_column].shift(1)  # Shift the activity column down to compare with previous row
    df = df[(df[activity_column] != df['prev_activity']) | (df['prev_activity'].isna())]
    df.drop(columns='prev_activity', inplace=True)  # Clean up temporary column

    # Re-sort if necessary (though it may not be if nothing has changed that affects order)
    df.sort_values(by=['visit', 'timestamp', 'artificial_order', 'activity_ML', 'activity_LL'], inplace=True)

    # Remove the artificial_order column and reset index to get new event IDs
    df.reset_index(drop=True, inplace=True)
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'event_id'}, inplace=True)

    return df