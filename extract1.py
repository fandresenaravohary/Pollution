import requests
import pandas as pd
from datetime import datetime, timezone


    # Fonction pour récupérer les données de qualité de l'air
def first_extract():
            # Remplacez 'YOUR_API_KEY' par votre clé API réelle
    API_KEY = 'YOUR_API_KEY'
    BASE_URL = 'http://api.openweathermap.org/data/2.5/air_pollution/history'

    # Lire les fichiers CSV
    demographic_data = pd.read_csv('/home/voahary/airflow/dags/Demographic_Data.csv')
    geographic_data = pd.read_csv('/home/voahary/airflow/dags/Geographic_Data.csv')

    def get_air_quality_data(lat, lon, start, end):
        params = {
            'lat': lat,
            'lon': lon,
            'start': start,
            'end': end,
            'appid': API_KEY
        }
        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erreur de l'API pour les coordonnées ({lat}, {lon}): {response.status_code}")
            return None

    # Fonction pour extraire les données de pollution
    def extract_pollution_data(api_data):
        try:
            pollution_entries = api_data['list']
            results = []
            for entry in pollution_entries:
                timestamp = entry['dt']
                date_time = datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                aqi = entry['main']['aqi']
                components = entry['components']
                data = {
                    'DateTime': date_time,
                    'AQI': aqi,
                    'PM2.5': components.get('pm2_5'),
                    'PM10': components.get('pm10'),
                    'O3': components.get('o3'),
                    'NO2': components.get('no2'),
                    'SO2': components.get('so2'),
                    'CO': components.get('co')
                }
                results.append(data)
            return results
        except (KeyError, IndexError):
            return None

        # Ajout des colonnes de latitude et longitude aux données démographiques
    latitude_longitude = {
        'Los Angeles': (34.0522, -118.2437),
        'Paris': (48.8566, 2.3522),
        'Tokyo': (35.6895, 139.6917),
        'Antananarivo': (-18.8792, 47.5079),
        'Nairobi': (-1.286389, 36.817223),
        'Lima': (-12.0464, -77.0428)
    }

    # Période pour laquelle récupérer les données (exemple: pour 2024-08-10)
    start_timestamp = 1704076200  # Remplacez par le timestamp de début
    end_timestamp = 1735606500    # Remplacez par le timestamp de fin

    # Collecte des données de pollution pour chaque ville
    pollution_data = []

    for index, row in demographic_data.iterrows():
        location = row['Location']
        lat, lon = latitude_longitude[location]
        api_data = get_air_quality_data(lat, lon, start_timestamp, end_timestamp)
        if api_data:
            pollution_entries = extract_pollution_data(api_data)
            if pollution_entries:
                for entry in pollution_entries:
                    entry['Location'] = location
                    pollution_data.append(entry)

    # Convertir en DataFrame
    pollution_df = pd.DataFrame(pollution_data)

    # Fusionner les données démographiques, géographiques et de pollution
    combined_data = pd.merge(demographic_data, pollution_df, on='Location')
    combined_data = pd.merge(combined_data, geographic_data, on='Location')

    # Sauvegarder le résultat dans un nouveau fichier CSV
    combined_data.to_csv('Combined_Data.csv', index=False)

    print(combined_data)

first_extract()
