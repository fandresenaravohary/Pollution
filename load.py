import pandas as pd
from transform import transform_data

def save_cleaned_data(combined_data_cleaned):
    # Sauvegarde des données nettoyées et transformées
    combined_data_cleaned.to_csv('Cleaned_Combined_Data.csv', index=False)

    print("Les données nettoyées et transformées ont été sauvegardées dans 'Cleaned_Combined_Data.csv'.")
    print(combined_data_cleaned.head())

if __name__ == "__main__":
    # Appel de la fonction de transformation pour obtenir les données
    combined_data_cleaned = transform_data()
    
    # Sauvegarde des données
    save_cleaned_data(combined_data_cleaned)
