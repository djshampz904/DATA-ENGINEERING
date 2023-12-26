import re
import csv

# Read the SQL script file
with open('world_mysql_script.sql', 'r') as file:
    script_content = file.read()

# Extract table structure information using regular expressions
table_structure_pattern = r'CREATE TABLE `(.*?)` \((.*?)\);'
table_structure_matches = re.findall(table_structure_pattern, script_content, re.DOTALL)

# Write table structure information to a CSV file

# Open a CSV file for writing
with open('table_structure.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Table Name', 'Column Name', 'Data Type', 'Nullable', 'Default Value'])

    # Iterate over each table structure match
    for table_name, table_structure in table_structure_matches:
        # Define a regular expression pattern to extract column information
        column_pattern = r'`(.*?)` (.*?) (.*?) (.*?),'
        column_matches = re.findall(column_pattern, table_structure)

        # Iterate over each column match
        for column_name, data_type, nullable, default_value in column_matches:
            # Write the table structure information to the CSV file
            writer.writerow([table_name, column_name, data_type, nullable, default_value])

# Print a success message
print("Table structure information has been written to table_structure.csv.")