
import csv
import os

def export_to_csv(conn, table_name, output_dir, file_name):
    """
    Exports the anonymized data from the database to a CSV file.
    Creates the output directory if it doesn't exist.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"?? Created directory: {output_dir}")

    filepath = os.path.join(output_dir, file_name)
    cur = conn.cursor()

    # Fetch all data from the anonymized table
    cur.execute(f"SELECT * FROM {table_name}")
    rows = cur.fetchall()

    # Get column names from the cursor description
    colnames = [desc[0] for desc in cur.description]

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Write the header
        writer.writerow(colnames)
        # Write the data
        writer.writerows(rows)

    cur.close()
    print(f"?? Data successfully exported to: {filepath}")