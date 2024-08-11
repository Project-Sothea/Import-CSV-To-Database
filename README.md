### ImportCSVToDatabase
This is a simple Python script that reads an exported CSV file and imports it into a PostgreSQL database.  
The script uses the `pandas` library to read the CSV file and the `sqlalchemy` library to connect to the PostgreSQL database.

This script allows importing existing CSV files to the database. The CSV file must be in the correct format, with the correct headers, and the correct order of columns.  
Note: This feature is not accessible to users since it should be rarely invoked, only to reset the database to a known state in the event of system failures :O  

Some desired properties of the import to DB feature include:
- Ability to use exported csv files from /export-db as a backup
- Easy to use and debug (If I ever have to use this feature, I need it to be quick to minimise downtime)

### Usage
1. Clone the repository.
2. Install the required libraries.
3. Update the database connection details.
4. Place the exported CSV file in the /csv directory.
5. Run the script by doing `python3 main.py`.

### CAUTION
- In order to prevent accidentally running the script and resetting the database, the script will only work if the existing tables are cleared.
- To clear the database, enter the Query Editor in pgAdmin and run the following SQL command manually:
```sql
TRUNCATE TABLE admin CASCADE;
```
