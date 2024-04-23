import pandas as pd
from pathlib import Path
from utils import load_data, merge_dataframes
from transformation import process_dataframes, join_vocabulary_data, device_mapping, create_ocel_event, preprocess_ocel_data, sort_ocel_data
from event_abstraction import abstract_events

if __name__ == "__main__":
    data_path = Path("./Data/")
    result_path = Path("./Results/")

    datasets = {
        'condition': load_data(data_path / "condition_occurrence.csv"),
        'device': load_data(data_path / "device_exposure.csv"),
        'drug': load_data(data_path / "drug_exposure.csv"),
        'measurement': load_data(data_path / "measurement.csv"),
        'observation': load_data(data_path / "observation.csv"),
        'person': load_data(data_path / "person.csv"),
        'procedure': load_data(data_path / "procedure_occurrence.csv"),
        'visit': load_data(data_path / "visit_occurrence.csv"),
        'voca_con': load_data(data_path / "concept.csv"),
        'voca_class': load_data(data_path / "concept_class.csv")
    }

    enriched_data = {}
    visit_data = datasets['visit']
    for name in ['condition', 'device', 'drug', 'measurement', 'observation', 'procedure', 'visit']:
        merged_data = merge_dataframes(visit_data, datasets[name], 'visit_occurrence_id')
        enriched_data[name] = join_vocabulary_data(merged_data, datasets['voca_con'], datasets['voca_class'], f"{name}_concept_id")
        print(f"Data merged and enriched for {name}")

    transformations = [
            (["condition_start_datetime", "condition_end_datetime"], ["condition_start_datetime", "condition_end_datetime"]),
            (["device_exposure_start_datetime", "device_exposure_end_datetime"], ["device_exposure_start_datetime", "device_exposure_end_datetime", "unique_device_id"]),
            (["drug_exposure_start_datetime", "drug_exposure_end_datetime"], ["drug_exposure_start_datetime", "drug_exposure_end_datetime"]),
            (["measurement_datetime"], ["measurement_datetime"]),
            (["observation_datetime"], ["observation_datetime"]),
            (["procedure_datetime"], ["procedure_datetime"]),
            ()
        ]

    key_columns = ['visit_occurrence_id', 'person_id', 'visit_start_datetime', 'visit_end_datetime', 'provider_id_y', 'domain_id', 'concept_class_name', 'concept_name']

    # Process all enriched data
    processed_data = process_dataframes(enriched_data, transformations, key_columns)

    OMOPCDM4PM_df = {}
    for name in ['condition', 'drug', 'measurement', 'observation', 'procedure', 'visit']:
        OMOPCDM4PM_df[name] = device_mapping(processed_data[name], processed_data['device'])

    common_columns = {
        'static_cols': {
            'visit': 'visit_occurrence_id',
            'patient': 'person_id',
            'provider': 'provider_id_y'
        },
        'activity_cols': {
            'activity_ML': 'concept_class_name',
            'activity_LL': 'concept_name'
        }
    }

    configurations = {
        'condition': {
            'source_df': OMOPCDM4PM_df['condition'],
            'activity_type': 'condition',
            'timestamp_cols': {
                'timestamp': 'condition_start_datetime',
                'event_start_timestamp': 'condition_start_datetime',
                'event_end_timestamp': 'condition_end_datetime'
            },
            'provider_cols': {
                'provider': 'provider_id_y'
            },
            'device_cols': {
                'device_DDD': 'DDD',
                'device_DAS': 'DAS',
                'device_INJ': 'INJ'
            },
            'is_visit_event' : False
        },
        'drug': {
            'source_df': OMOPCDM4PM_df['drug'],
            'activity_type': 'drug',
            'timestamp_cols': {
                'timestamp': 'drug_exposure_start_datetime',
                'event_start_timestamp': 'drug_exposure_start_datetime',
                'event_end_timestamp': 'drug_exposure_end_datetime'
            },
            'provider_cols': {
                'provider': 'provider_id_y'
            },
            'device_cols': {
                'device_DDD': 'DDD',
                'device_DAS': 'DAS',
                'device_INJ': 'INJ'
            },
            'is_visit_event' : False
        },
        'measurement': {
            'source_df': OMOPCDM4PM_df['measurement'],
            'activity_type': 'measurement',
            'timestamp_cols': {
                'timestamp': 'measurement_datetime',
                'event_start_timestamp': 'measurement_datetime',
                'event_end_timestamp': 'measurement_datetime'
            },
            'provider_cols': {
                'provider': 'provider_id_y'
            },
            'device_cols': {
                'device_DDD': 'DDD',
                'device_DAS': 'DAS',
                'device_INJ': 'INJ'
            },
            'is_visit_event' : False
        },
        'observation': {
            'source_df': OMOPCDM4PM_df['observation'],
            'activity_type': 'observation',
            'timestamp_cols': {
                'timestamp': 'observation_datetime',
                'event_start_timestamp': 'observation_datetime',
                'event_end_timestamp': 'observation_datetime'
            },
            'provider_cols': {
                'provider': 'provider_id_y'
            },
            'device_cols': {
                'device_DDD': 'DDD',
                'device_DAS': 'DAS',
                'device_INJ': 'INJ'
            },
            'is_visit_event' : False
        },
        'procedure': {
            'source_df': OMOPCDM4PM_df['procedure'],
            'activity_type': 'procedure',
            'timestamp_cols': {
                'timestamp': 'procedure_datetime',
                'event_start_timestamp': 'procedure_datetime',
                'event_end_timestamp': 'procedure_datetime'
            },
            'provider_cols': {
                'provider': 'provider_id_y'
            },
            'device_cols': {
                'device_DDD': 'DDD',
                'device_INJ': 'INJ'
            },
            'is_visit_event' : False
        },
        'visit_start': {
            'source_df': OMOPCDM4PM_df['visit'],
            'activity_type': 'visit_start',
            'timestamp_cols': {
                'timestamp': 'visit_start_datetime',
                'event_start_timestamp': 'visit_start_datetime',
                'event_end_timestamp': 'visit_start_datetime'
            },
            'provider_cols': {
                'provider': 'provider_id_y'
            },
            'device_cols': None,
            'is_visit_event' : True
        },
        'visit_end': {
            'source_df': OMOPCDM4PM_df['visit'],
            'activity_type': 'visit_end',
            'timestamp_cols': {
                'timestamp': 'visit_end_datetime',
                'event_start_timestamp': 'visit_end_datetime',
                'event_end_timestamp': 'visit_end_datetime'
            },
            'provider_cols': {
                'provider': 'provider_id_y'
            },
            'device_cols': None,
            'is_visit_event' : True
        }
    }

    all_events = [create_ocel_event(config['source_df'],
                                    config['activity_type'],
                                    config['timestamp_cols'],
                                    common_columns,
                                    config.get('provider_cols', False),
                                    config.get('device_cols', False),
                                    config.get('is_visit_event', False)) for config in configurations.values()]

    OCEL_df = pd.concat(all_events, ignore_index=True)
    processed_OCEL = preprocess_ocel_data(OCEL_df)
    sorted_OCEL = sort_ocel_data(processed_OCEL)
    sorted_OCEL.to_csv(result_path / "OCEL_LL.csv", index=False)
    OCEL_HL = abstract_events(sorted_OCEL, 'activity_HL')
    OCEL_ML = abstract_events(sorted_OCEL, 'activity_ML')
    OCEL_ML.to_csv(result_path / "OCEL_ML.csv", index=False)
    OCEL_HL.to_csv(result_path / "OCEL_HL.csv", index=False)