import pandas as pd
from sqlalchemy import create_engine

# open output.csv file and read out the columns
df = pd.read_csv('csv/output.csv')
csv_cols = df.columns

# maps the expected column names in the csv to the actual schema column names
csv_schema_cols = {
    "admin": {
        "id": "id",
        "vid": "vid",
        "family_group": "family_group",
        "reg_date": "reg_date",
        "queue_no": "queue_no",
        "name": "name",
        "khmer_name": "khmer_name",
        "dob": "dob",
        "age": "age",
        "gender": "gender",
        "village": "village",
        "contact_no": "contact_no",
        "pregnant": "pregnant",
        "last_menstrual_period": "last_menstrual_period",
        "drug_allergies": "drug_allergies",
        "sent_to_id": "sent_to_id",
        "photo": "photo"
    },
    "pastmedicalhistory": {
        "id": "id",
        "vid": "vid",
        "tuberculosis": "tuberculosis",
        "diabetes": "diabetes",
        "hypertension": "hypertension",
        "hyperlipidemia": "hyperlipidemia",
        "chronic_joint_pains": "chronic_joint_pains",
        "chronic_muscle_aches": "chronic_muscle_aches",
        "sexually_transmitted_disease": "sexually_transmitted_disease",
        "specified_stds": "specified_stds",
        "pmh_others": "others",
    },
    "socialhistory": {
        "id": "id",
        "vid": "vid",
        "past_smoking_history": "past_smoking_history",
        "no_of_years": "no_of_years",
        "current_smoking_history": "current_smoking_history",
        "cigarettes_per_day": "cigarettes_per_day",
        "alcohol_history": "alcohol_history",
        "how_regular": "how_regular",
    },
    "vitalstatistics": {
        "id": "id",
        "vid": "vid",
        "temperature": "temperature",
        "spo2": "spo2",
        "systolic_bp1": "systolic_bp1",
        "diastolic_bp1": "diastolic_bp1",
        "systolic_bp2": "systolic_bp2",
        "diastolic_bp2": "diastolic_bp2",
        "avg_systolic_bp": "avg_systolic_bp",
        "avg_diastolic_bp": "avg_diastolic_bp",
        "hr1": "hr1",
        "hr2": "hr2",
        "avg_hr": "avg_hr",
        "rand_blood_glucose_mmoll": "rand_blood_glucose_mmoll",
        "rand_blood_glucose_mmollp": "rand_blood_glucose_mmollp",
    },
    "heightandweight": {
        "id": "id",
        "vid": "vid",
        "height": "height",
        "weight": "weight",
        "bmi": "bmi",
        "bmi_analysis": "bmi_analysis",
        "paeds_height": "paeds_height",
        "paeds_weight": "paeds_weight",
    },
    "visualacuity": {
        "id": "id",
        "vid": "vid",
        "l_eye_vision": "l_eye_vision",
        "r_eye_vision": "r_eye_vision",
        "additional_intervention": "additional_intervention",
    },
    "doctorsconsultation": {
        "id": "id",
        "vid": "vid",
        "healthy": "healthy",
        "msk": "msk",
        "cvs": "cvs",
        "respi": "respi",
        "gu": "gu",
        "git": "git",
        "eye": "eye",
        "derm": "derm",
        "dc_others": "others",
        "consultation_notes": "consultation_notes",
        "diagnosis": "diagnosis",
        "treatment": "treatment",
        "referral_needed": "referral_needed",
        "referral_loc": "referral_loc",
        "remarks": "remarks",
    }
}

# assert the sizes of the columns
assert(len(csv_cols) == 69)
assert(len(csv_schema_cols["admin"]) == 17)
assert(len(csv_schema_cols["pastmedicalhistory"]) == 11)
assert(len(csv_schema_cols["socialhistory"]) == 8)
assert(len(csv_schema_cols["vitalstatistics"]) == 15)
assert(len(csv_schema_cols["heightandweight"]) == 8)
assert(len(csv_schema_cols["visualacuity"]) == 5)
assert(len(csv_schema_cols["doctorsconsultation"]) == 17)

# For each row, split into multiple rows based on the category they belong to (admin, pastmedicalhistory, socialhistory, vitalstatistics, heightandweight, visualacuity, doctorsconsultation)
# and write to a new csv file in the same directory
# the first two columns are id and vid

def remove_null_rows(df: pd.DataFrame, columns_to_keep: list) -> pd.DataFrame:
    non_keep_columns = [col for col in df.columns if col not in columns_to_keep]
    return df[~df[non_keep_columns].isna().all(axis=1)]

def process_table(df: pd.DataFrame, schema: dict, table_name: str) -> pd.DataFrame:
    # Select the relevant columns and rename them
    subset_df = df[list(schema.keys())].rename(columns=schema, inplace=False)
    # Remove rows that are entirely null except for the id and vid columns
    subset_df = remove_null_rows(subset_df, ['id', 'vid'])
    return subset_df

def write_to_database(df: pd.DataFrame, table_name: str, engine) -> None:
    try:
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Data for table '{table_name}' has been written to the database.")
    except Exception as e:
        print(f"Error writing to table '{table_name}': {e}")

admin = process_table(df, csv_schema_cols["admin"], "admin")
pastmedicalhistory = process_table(df, csv_schema_cols["pastmedicalhistory"], "pastmedicalhistory")
socialhistory = process_table(df, csv_schema_cols["socialhistory"], "socialhistory")
vitalstatistics = process_table(df, csv_schema_cols["vitalstatistics"], "vitalstatistics")
heightandweight = process_table(df, csv_schema_cols["heightandweight"], "heightandweight")
visualacuity = process_table(df, csv_schema_cols["visualacuity"], "visualacuity")
doctorsconsultation = process_table(df, csv_schema_cols["doctorsconsultation"], "doctorsconsultation")

database_name = "patients"
host_name = "localhost"
user_name = "jieqiboh"
password = "postgres"
port_number = "5432"

# Format the connection string
connection_string = f'postgresql+psycopg2://{user_name}:{password}@{host_name}:{port_number}/{database_name}'

# Create the SQLAlchemy engine
engine = create_engine(connection_string)

# Write each DataFrame to its respective table
write_to_database(admin, "admin", engine)
write_to_database(pastmedicalhistory, "pastmedicalhistory", engine)
write_to_database(socialhistory, "socialhistory", engine)
write_to_database(vitalstatistics, "vitalstatistics", engine)
write_to_database(heightandweight, "heightandweight", engine)
write_to_database(visualacuity, "visualacuity", engine)
write_to_database(doctorsconsultation, "doctorsconsultation", engine)

print("CSV file has been successfully imported to the database.")