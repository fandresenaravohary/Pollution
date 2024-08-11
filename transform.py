import pandas as pd
from sklearn.preprocessing import MinMaxScaler

def transform_data(**context):  # Added context to use with Airflow XCom if needed
    try:
        combined_data = pd.read_csv('/home/voahary/airflow/dags/Combined_Data.csv')

        print(combined_data.head())

        print("Valeurs manquantes par colonne:")
        print(combined_data.isnull().sum())

        combined_data_cleaned = combined_data.dropna()
        combined_data_cleaned = combined_data_cleaned.drop_duplicates()

        scaler = MinMaxScaler()
        columns_to_normalize = ['AQI', 'PM2.5', 'PM10', 'O3', 'NO2', 'SO2', 'CO']
        combined_data_cleaned[columns_to_normalize] = scaler.fit_transform(combined_data_cleaned[columns_to_normalize])

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

        return combined_data_cleaned

    except Exception as e:
        print(f"Erreur dans la transformation des données: {e}")
        raise  # Remonte l'exception pour qu'Airflow puisse gérer le retry
