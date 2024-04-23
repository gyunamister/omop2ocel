# Transforming OMOP Common Data Models into Object-Centric Event Logs

This repository contains the implementation code for converting OMOP Common Data Models (CDM) into object-centric event logs suitable for object-centric process mining techniques.

## Data Preparation
To utilize our transformation method, access to the MIMIC-IV dataset is required. Since the MIMIC-IV dataset is protected under licensing agreements, it cannot be provided here directly.

Follow these steps to prepare the data:

1. Obtain access to the MIMIC-IV dataset by following the instructions provided at [PhysioNet MIMIC-IV](https://physionet.org/content/mimiciv/2.2/).
2. Convert the MIMIC-IV dataset into the OMOP CDM format using the conversion tool available at the [OHDSI/MIMIC GitHub repository](https://github.com/OHDSI/MIMIC).

After the conversion, place the following CSV files in the `./Data/` directory of this repository:
- `condition_occurrence.csv`
- `device_exposure.csv`
- `drug_exposure.csv`
- `measurement.csv`
- `person.csv`
- `procedure_occurrence.csv`
- `visit_occurrence.csv`
- `concept.csv`
- `concept_class.csv`

## How to Run
To execute the transformation script, first install the required Python packages and then run the `main.py` script as shown below:

```bash
pip install -r requirements.txt
python main.py
```

## Output
The script generates object-centric event logs in CSV format, which are stored in the `./Results` directory. These event logs are structured for direct use in process mining tools, facilitating a range of analyses from conformance checking to process discovery and enhancement.

## Citation
<!-- If you use the code in this repository for your research, please cite our paper as follows: -->

## License

## Contact
For questions and feedback regarding the code, please open an issue in this repository or contact us at gnpark[at]pads.rwth-aachen.de