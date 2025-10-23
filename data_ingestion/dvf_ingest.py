import os
import requests
from google.cloud import storage
from tqdm import tqdm

# === CONFIG ===
BUCKET_NAME = "datalake-immo-007"
KEY_PATH = os.path.expanduser("~/.gcp_keys/datalake-sa-key.json")

# === ANNÉES À TÉLÉCHARGER ===
ANNÉES = list(range(2020, 2026))  # De 2020 à 2025 inclus

def télécharger_fichier_dvf(année):
    """Télécharge et upload un fichier DVF pour une année donnée"""
    
    # Construction des URLs et noms de fichiers
    dvf_url = f"https://files.data.gouv.fr/geo-dvf/latest/csv/{année}/full.csv.gz"
    local_file = f"dvf_{année}.csv.gz"
    destination_blob = f"raw/dvf/dvf_{année}.csv.gz"
    
    print(f"\nTéléchargement du fichier DVF {année}...")
    print(f"URL: {dvf_url}")
    
    try:
        # === TÉLÉCHARGEMENT DU FICHIER ===
        response = requests.get(dvf_url, stream=True)
        response.raise_for_status()  # Vérifie que la requête a réussi
        
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024
        
        with open(local_file, "wb") as f, tqdm(
            desc=local_file, total=total_size, unit="iB", unit_scale=True
        ) as bar:
            for data in response.iter_content(block_size):
                bar.update(len(data))
                f.write(data)
        print(f"Téléchargement {année} terminé.")
        
        # === ENVOI SUR GCS ===
        print(f"Upload vers gs://{BUCKET_NAME}/{destination_blob}...")
        client = storage.Client.from_service_account_json(KEY_PATH)
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob)
        blob.upload_from_filename(local_file)
        print(f"Upload {année} terminé.")
        
        # === NETTOYAGE LOCAL ===
        os.remove(local_file)
        print(f"Fichier local {local_file} supprimé.")
        
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors du téléchargement pour {année}: {e}")
        return False
    except Exception as e:
        print(f"Erreur inattendue pour {année}: {e}")
        return False

# === EXÉCUTION PRINCIPALE ===
def main():
    print(f"Début du téléchargement des fichiers DVF pour les années {ANNÉES}")
    
    résultats = []
    for année in ANNÉES:
        succès = télécharger_fichier_dvf(année)
        résultats.append((année, succès))
    
    # === RAPPORT FINAL ===
    print(f"\n{'='*50}")
    print(f"RAPPORT FINAL")
    print(f"{'='*50}")
    
    années_succès = [année for année, succès in résultats if succès]
    années_échec = [année for année, succès in résultats if not succès]
    
    print(f"Années téléchargées avec succès: {années_succès}")
    if années_échec:
        print(f"Années en échec: {années_échec}")
    else:
        print("Tous les fichiers ont été téléchargés avec succès!")

    print(f"\nTotal: {len(années_succès)}/{len(ANNÉES)} fichiers traités")

if __name__ == "__main__":
    main()