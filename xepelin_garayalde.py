import os
from datetime import datetime, timedelta
import pandas as pd
from pandas.io import gbq
from dotenv import load_dotenv
import pysftp


# Load variables from the .env file
load_dotenv()

sftp_host = os.environ.get('SFTP_HOST_NAME')
sftp_user = os.environ.get('SFTP_USER')
sftp_pswd = os.environ.get('SFTP_PASSWORD')

gbq_project_id = os.environ.get('GBQ_PROJECT_ID')
gbq_dest_table = os.environ.get('GBQ_DESTINATION_TABLE')


def download_csv_from_sftp(filepath_at_sftp, filepath_at_local):

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    try:
        with pysftp.Connection(host=sftp_host, username=sftp_user, password=sftp_pswd, cnopts=cnopts) as sftp:

            print('Connection succesfully established')

            sftp.get(filepath_at_sftp, filepath_at_local)

        # connection closed automatically at the end of the with statement

    except FileNotFoundError:
        print(f"The file: {filepath_at_sftp} is not at the SFTP yet.")
        raise SystemExit(1)
    except Exception as e:
        print(f"Error downloading CSV from SFTP: {e}")
        raise SystemExit(1)


def process_csv(csv_path):
    try:
        df = pd.read_csv(csv_path).transpose()
        df.columns = df.iloc[0]     # Asign first row as header
        df = df[1:]

        expected_columns = ['orderId', 'customerId', 'createdAt', 'amount']

        # Check if all expected columns are present
        if all(column in df.columns for column in expected_columns):

            # Convert it from string to a datetime object first and extract date from the datetime object
            df['date'] = pd.to_datetime(df['createdAt']).dt.date

            df['amount'] = df['amount'].astype(int)      # Change datatype

            grouped = df.groupby(['customerId', 'date'])['amount'].agg(
                ['sum', 'mean', 'median']).reset_index()

            grouped.columns = ['customerId', 'date',
                               'totalAmount', 'avgAmount', 'medianAmount']

            print("CSV has been processed correctly.")

            print(grouped)

            return grouped

        else:
            print(
                "One or more of these rows are missing: 'orderId','customerId', 'createdAt', 'amount'.")

    except Exception as e:
        print(f"Error processing CSV: {e}")
        raise SystemExit(1)


def upload_to_bigquery(csv_path):

    # Specify the table schema
    gbq_insights_schema = [
        {'name': 'customerId',      'type': 'STRING'},
        {'name': 'date',            'type': 'DATE'},
        {'name': 'totalAmount',     'type': 'FLOAT'},
        {'name': 'avgAmount',       'type': 'FLOAT'},
        {'name': 'medianAmount',    'type': 'FLOAT'}
    ]

    try:

        # I have implemented a simple append method. If the project requirements allow for it,
        # a bettter and more robust strategy would be to update the existing data overwriting by date.

        # Upload the DataFrame to BigQuery with schema
        processed_data.to_gbq(project_id=gbq_project_id,      # Replace accordingly
                              destination_table=gbq_dest_table,
                              if_exists='append',
                              table_schema=gbq_insights_schema)

        print("Data uploaded to BigQuery was successfull.")

    except Exception as e:
        print(f"Error uploading data to BigQuery: {e}")
        raise SystemExit(1)


if __name__ == "__main__":

    # today = datetime.now().strftime("%Y-%m-%d")
    today = '2023-10-01'

    filepath_at_sftp = f'''orders_{today}.csv'''
    filepath_at_local = 'orders_temp.csv'

    download_csv_from_sftp(filepath_at_sftp, filepath_at_local)

    processed_data = process_csv(filepath_at_local)

    upload_to_bigquery(processed_data)


# ¿Qué pasa cuando el server está caído? Se generará una excepción indicando el mensaje de error
# ¿Qué pasa si el archivo aún no fue cargado? ->  se genera un mensaje de error informando que el archivo no se encontro
# ¿Qué pasa si ejecuto el script múltiples veces en el mismo día? -> se duplican los datos
# ¿Qué pasa si pierdo la tabla y necesito re generarla desde X día? -> se puede crear un script que ejecute un loop variando el nombre del archivo desde la fecha requerida
# ¿Qué pasa si corro dos veces el mismo script al mismo tiempo?
# ¿Qué pasa si el script debe ejecutar en un ambiente sin acceso a disco? -> se puede modificar el codigo para pasar directo de SFTP a df y de ahi a GBQ
