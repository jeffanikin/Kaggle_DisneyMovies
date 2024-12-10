import os
import shutil
import subprocess
import pandas as pd
import numpy as np
import hashlib
from sqlalchemy import create_engine
import pyodbc
import sys

# Step 1: Create .kaggle directory
os.makedirs(os.path.expanduser("~/.kaggle"), exist_ok=True)

# Step 2: Copy the kaggle.json file
shutil.copy(
    r"C:\Mac\Home\Downloads\kaggle.json", r"C:\Users\anikindj\.kaggle\kaggle.json"
)

# Step 3: Change directory to .kaggle
os.chdir(r"C:\Users\anikindj\.kaggle")

# Step 4: List contents of .kaggle directory
print("Contents of .kaggle directory:")
for item in os.listdir():
    print(item)


# Step 1: Custom Logger to capture print outputs
class PrintLogger:
    def __init__(self):
        self.logs = []

    def log(self, title, message):
        self.logs.append({"Title": title, "Message": message})

    def export_to_csv(self, file_name):
        pd.DataFrame(self.logs).to_csv(file_name, index=False)
        print(f"Logs exported to {file_name}")

    # Initialize logger


logger = PrintLogger()

df = pd.read_csv(r"C:\Users\anikindj\data\walt_disney_movies.csv")
logger.log(
    "Data Loaded",
    f"Loaded DataFrame with {len(df)} rows and {len(df.columns)} columns.",
)

# Rename the column
if "Unnamed: 0" in df.columns:
    df.rename(columns={"Unnamed: 0": "ID"}, inplace=True)
    print("Column 'Unnamed: 0' has been renamed to 'ID'.")

# Missing values
missing_values = df.isnull().sum()
logger.log("Data Quality: Missing Values", missing_values.to_string())

# Duplicate rows
duplicate_rows = df.duplicated().sum()
logger.log("Data Quality: Duplicates", f"Number of duplicate rows: {duplicate_rows}")

# 3. Check Data Integrity
print("Data Integrity Checks:")
if "title" in df.columns:
    print(f"Number of unique titles: {df['title'].nunique()}")

# Step 4: Data Security
if "Country" in df.columns:
    logger.log("Data Security", "Masking 'Country' column for security...")
    df["Country"] = df["Country"].apply(
        lambda x: hashlib.sha256(x.encode()).hexdigest() if isinstance(x, str) else x
    )

# Schema Validation
expected_columns = {
    "title": "object",
    "Production company": "object",
    "Country": "object",
    "Language": "object",
    "Running time": "float64",
    "Budget": "int64",
    "Box office": "float64",
    "Release date": "object",
    "imdb": "float64",
    "metascore": "int64",
    "rotten_tomatoes": "object",
    "Directed by": "object",
    "Produced by": "object",
    "Based on": "object",
    "Starring": "object",
    "Music by": "object",
    "Distributed by": "object",
    "Cinematography": "object",
    "Edited by": "object",
    "Screenplay by": "foat",
}

missing_columns = [col for col in expected_columns if col not in df.columns]
if missing_columns:
    logger.log("Schema Validation", f"Missing columns: {missing_columns}")

# Database connection

# Upload to SQL
server = r"(localdb)\mylocaldb"
database = "kaggle"
connection_string = (
    f"mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
)
engine = create_engine(connection_string)
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
)

table_name = "disney_Movies"
cursor = conn.cursor()
cursor.execute(f"IF OBJECT_ID ('{table_name}', 'U') IS NOT NULL SELECT 1 ELSE SELECT 0")
table_exists = cursor.fetchone()[0]

if table_exists:
    backup_table = f"{table_name}_backup"
    logger.log("SQL Backup", f"Creating a backup for table: {table_name}")
    cursor.execute(
        f"IF OBJECT_ID ('{backup_table}', 'U') IS NOT NULL DROP TABLE {backup_table}"
    )
    cursor.execute(f"SELECT * INTO {backup_table} FROM {table_name}")
    conn.commit()

try:
    # Rename the column in the DataFrame
    if "Unnamed: 0" in df.columns:
        df.rename(columns={"Unnamed: 0": "ID"}, inplace=True)
        print("Column 'Unnamed: 0' has been renamed to 'ID'.")

    # Ensure the 'ID' column has no null values
    if "ID" in df.columns:
        print(f"Checking for null values in 'ID' column: {df['ID'].isnull().sum()}")
        if df["ID"].isnull().sum() > 0:
            print("Filling null values in 'ID' with unique identifiers.")
            df["ID"] = pd.factorize(df["ID"])[0]  # Replace nulls with unique integers

    # Upload DataFrame to SQL
    df.to_sql(
        table_name,
        schema=None,
        con=engine,
        chunksize=20,
        method="multi",
        index=False,
        if_exists="replace",
    )
    print(f"Successfully uploaded DataFrame to table: {table_name}")

    # Make the ID column non-nullable
    print("Making 'ID' column non-nullable.")
    cursor.execute(
        f"""
        ALTER TABLE [{table_name}]
        ALTER COLUMN [ID] INT NOT NULL;
    """
    )
    conn.commit()

    # Set 'ID' as the primary key
    print("Setting 'ID' as the primary key.")
    cursor.execute(
        f"""
        ALTER TABLE [{table_name}]
        ADD CONSTRAINT [pk_{table_name}_ID]
        PRIMARY KEY ([ID]);
    """
    )
    conn.commit()
    print("Primary key set successfully for 'ID' column.")

except Exception as ex:
    print(f"Error uploading table: {table_name}. Error: {ex}")
finally:
    conn.close()

# Step 7: Post-upload Validation
query = f"SELECT * FROM {table_name}"
df_uploaded = pd.read_sql_query(query, con=engine)
logger.log(
    "SQL Validation: Rows Loaded",
    f"Loaded {len(df_uploaded)} rows from the database table: {table_name}",
)

missing_values_uploaded = df_uploaded.isnull().sum()
logger.log("SQL Validation: Missing Values", missing_values_uploaded.to_string())

duplicates_uploaded = df_uploaded.duplicated().sum()
logger.log(
    "SQL Validation: Duplicate Rows", f"Number of duplicate rows: {duplicates_uploaded}"
)

# Export all captured logs to CSV
logger.export_to_csv("validation_log.csv")
