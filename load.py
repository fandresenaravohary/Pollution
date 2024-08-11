import pandas as pd

def save_cleaned_data(**context):
    # Récupération des données transformées via XCom
    ti = context['ti']
    combined_data_cleaned = ti.xcom_pull(task_ids='transform_data')

    if combined_data_cleaned is None:
        raise ValueError("Aucune donnée transformée reçue pour la sauvegarde.")

    # Sauvegarde des données nettoyées et transformées
    combined_data_cleaned.to_csv('/home/voahary/airflow/dags/Cleaned_Combined_Data.csv', index=False)

    print("Les données nettoyées et transformées ont été sauvegardées dans 'Cleaned_Combined_Data.csv'.")
    print(combined_data_cleaned.head())
