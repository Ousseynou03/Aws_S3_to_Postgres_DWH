import boto3
import pandas as pd

# Paramètres S3
bucket_name = 'test-bucket-v1.0.0-386510763288'
folders = ['source_crm/', 'source_erp/']  # dossiers à parcourir


#Lecture et affichages des fichiers depuis AWS S3
def list_and_read_csv():
    s3 = boto3.client('s3', region_name='eu-west-3')
    csv_files = {} # Dictionnaire pour stocker les DataFrames

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
                csv_files[file_key] = None
                # Lire les 5 premières lignes de chaque ficher csv
                obj_response = s3.get_object(Bucket=bucket_name, Key=file_key)
                df = pd.read_csv(obj_response['Body'])
                print(df.head(5))

    return csv_files


def process_data():
    # Fonction de traitement des données (à implémenter selon les besoins)
    # ici on a besoin pour chaque csv d'effectuer une insertion dans la bdd postgres
    
    pass




# Exécution
list_and_read_csv()