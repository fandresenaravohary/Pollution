import pandas as pd
from sklearn.preprocessing import MinMaxScaler


    # Charger les données combinées
def transform_data():
    combined_data = pd.read_csv('Combined_Data.csv')

    # Afficher les premières lignes pour vérification
    print(combined_data.head())

    # Vérifier les valeurs manquantes
    print("Valeurs manquantes par colonne:")
    print(combined_data.isnull().sum())

    # Nettoyage des données
    # Suppression des lignes avec des valeurs manquantes
    combined_data_cleaned = combined_data.dropna()

    # Suppression des doublons
    combined_data_cleaned = combined_data_cleaned.drop_duplicates()

    # Normalisation des colonnes spécifiques
    scaler = MinMaxScaler()
    columns_to_normalize = ['AQI', 'PM2.5', 'PM10', 'O3', 'NO2', 'SO2', 'CO']
    combined_data_cleaned[columns_to_normalize] = scaler.fit_transform(combined_data_cleaned[columns_to_normalize])

    # Création de catégories de pollution
    def categorize_aqi(aqi):
        if aqi <= 50:
            return 'Good'
        elif aqi <= 100:
            return 'Moderate'
        elif aqi <= 150:
            return 'Unhealthy for Sensitive Groups'
        elif aqi <= 200:
            return 'Unhealthy'
        elif aqi <= 300:
            return 'Very Unhealthy'
        else:
            return 'Hazardous'

    combined_data_cleaned['AQI_Category'] = combined_data_cleaned['AQI'].apply(categorize_aqi)

    # Sélectionner uniquement les colonnes numériques pour l'agrégation
    numeric_columns = ['AQI', 'PM2.5', 'PM10', 'O3', 'NO2', 'SO2', 'CO']
    city_aggregation = combined_data_cleaned.groupby('Location')[numeric_columns].mean()

    return combined_data_cleaned  # Retourne les données nettoyées

# Si vous voulez exécuter la transformation dans ce fichier
if __name__ == "__main__":
    cleaned_data = transform_data()
    print(cleaned_data.head())