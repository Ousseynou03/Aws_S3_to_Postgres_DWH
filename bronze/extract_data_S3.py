import os
from dotenv import load_dotenv
import boto3
import pandas as pd
from sqlalchemy import create_engine

# --- Chargement du fichier .env ---
load_dotenv()

# --- Paramètres S3 ---
bucket_name = os.getenv('BUCKET_NAME')
folders = ['source_crm/', 'source_erp/']
region_name = os.getenv('AWS_REGION')

# --- Paramètres PostgreSQL ---
pg_user = os.getenv('PG_USER')
pg_password = os.getenv('PG_PASSWORD')
pg_host = os.getenv('PG_HOST')
pg_port = os.getenv('PG_PORT')
pg_db = os.getenv('PG_DB')

# --- Connexion PostgreSQL ---
engine = create_engine(
    f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
)

# --- Lecture et affichage des fichiers depuis AWS S3 ---
def list_and_read_csv():
    s3 = boto3.client('s3', region_name=region_name)
    csv_files = {}  # Dictionnaire pour stocker les DataFrames

    for folder in folders:
        print(f"\n📂 Dossier : {folder}")
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder)

        if 'Contents' not in response:
            print(f"Aucun fichier trouvé dans {folder}")
            continue

        for obj in response['Contents']:
            file_key = obj['Key']
            if file_key.endswith('.csv'):
                print(f"✅ Fichier CSV trouvé : {file_key}")
                obj_response = s3.get_object(Bucket=bucket_name, Key=file_key)
                df = pd.read_csv(obj_response['Body'])

                # Stocker le DataFrame dans le dictionnaire
                csv_files[file_key] = df

                # Afficher les 5 premières lignes
                print(df.head(5))

    return csv_files


# --- Traitement des données : insertion dans PostgreSQL ---
def process_data(csv_files):
    for file_key, df in csv_files.items():
        # Extraire le nom du fichier sans extension
        base_name = file_key.split("/")[-1].replace(".csv", "").lower()
        
        # Identifier la source (crm ou erp)
        if "crm" in file_key:
            table_name = f"crm_{base_name}"
        elif "erp" in file_key:
            table_name = f"erp_{base_name}"
        else:
            table_name = base_name  # fallback

        print(f"\n🗃️ Insertion du fichier {file_key} dans la table bronze.{table_name} ...")

        # Insertion dans la table du schéma bronze
        df.to_sql(
            table_name,
            con=engine,
            schema="bronze",      
            if_exists="replace",
            index=False
        )

    print("\n✅ Insertion terminée avec succès dans le schéma bronze !")



# --- Exécution ---
if __name__ == "__main__":
    csv_files = list_and_read_csv()
    process_data(csv_files)
