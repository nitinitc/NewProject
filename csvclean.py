import pandas as pd
import sys

# Read the CSV file path from command line argument
if len(sys.argv) != 2:
    print("Usage: python csv_new_clean.py <input_csv_file>")
    sys.exit(1)

csv_file_path = sys.argv[1]

# Read the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file_path)

# Function to clean and validate data before insertion
def clean_and_validate(row):
    # Replace NaN values with None for proper insertion into MySQL
    row = [None if pd.isnull(value) else value for value in row]

    # Other cleansing/validation steps can be added here, such as checking for invalid values, data formatting, etc.
    return row

# Function to check for missing values
def check_missing_values(df):
    missing_values = df.isnull().sum()
    print("Missing Values:")
    print(missing_values)

# Function to check for duplicate records
def check_duplicate_records(df):
    duplicate_records = df[df.duplicated()]
    if len(duplicate_records) > 0:
        print("Duplicate Records:")
        print(duplicate_records)
    else:
        print("No Duplicate Records found.")

# Remove duplicate records from the DataFrame
df = df.drop_duplicates()

# Define the output CSV file path
output_csv_file_path = "/home/ec2-user/UKUSMar/nitin/sparkcode/cleaned_member_schemes.csv"

try:
    # Save the cleaned DataFrame to a CSV file
    df.to_csv(output_csv_file_path, index=False)

    # Print the path of the output CSV file
    print("Cleaned CSV file saved to:", output_csv_file_path)

except Exception as e:
    print("Error:", e)

