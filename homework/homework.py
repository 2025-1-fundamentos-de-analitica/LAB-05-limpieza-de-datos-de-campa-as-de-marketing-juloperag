"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import os
import zipfile
import pandas as pd

def clean_campaign_data():

    """
    Contenido de archivos cvs
    ,client_id,
    age,
    job,
    marital,
    education,
    credit_default,
    mortgage,
    month,
    day,
    contact_duration,
    number_contacts,
    previous_campaign_contacts,
    previous_outcome,
    cons_price_idx,
    euribor_three_months,
    campaign_outcome

    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months

    """
    #Diccionario para mapear fecha
    month_map = {'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
    'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
    'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'}
    #Definicion de Columnas 
    col_client = ["client_id", "age","job","marital","education","credit_default","mortgage"]
    col_campaing = ["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts","previous_outcome","campaign_outcome", "month", "day"]
    col_economics = ["client_id", "cons_price_idx", "euribor_three_months"]
    #Definicion de df
    client_df = pd.DataFrame(columns=col_client)
    campaign_df = pd.DataFrame(columns=col_campaing)
    economics_df = pd.DataFrame(columns=col_economics)
    #Direcciones
    output_dir = "./files/output/"
    input_dir = "./files/input/"
    #Gestion de archivos
    for fzip in os.listdir(input_dir):
        with zipfile.ZipFile(os.path.join(input_dir,fzip)) as z:
            for cvs_name in z.namelist():
                with z.open(cvs_name) as f:
                    #Abrir diccionario
                    df = pd.read_csv(f)
                    #Repartimos columnas a los dataframe
                    client_df = pd.concat([client_df, df[col_client]], ignore_index=True)
                    campaign_df = pd.concat([campaign_df, df[col_campaing]], ignore_index=True)
                    economics_df = pd.concat([economics_df, df[col_economics]], ignore_index=True)

    #Limpieza de datos
    #Limpieza client
    client_df.job = client_df.job.str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
    client_df.education = client_df.education.str.replace(".", "_", regex=False)
    client_df.loc[client_df["education"] == "unknown", "education"] = pd.NA
    client_df.credit_default = client_df.credit_default.apply(
        lambda x: pd.NA if pd.isna(x) else (1 if x == "yes" else 0)
    )
    client_df.mortgage = client_df.mortgage.apply(
        lambda x: pd.NA if pd.isna(x) else (1 if x == "yes" else 0)
    )
    #Limpieza campaign
    campaign_df.previous_outcome = campaign_df.previous_outcome.apply(lambda x: 1 if x=="success" else 0)
    campaign_df.campaign_outcome = campaign_df.campaign_outcome.apply(lambda x: 1 if x=="yes" else 0)
    campaign_df["last_contact_date"] = campaign_df.apply(lambda row: f"2022-{month_map[row['month'].lower()]}-{int(row['day']):02d}", axis=1)
    campaign_df = campaign_df.drop(columns=["month","day"])

    # Crear carpeta de salida si no existe
    os.makedirs(output_dir, exist_ok=True)
    #Guardar el dataframe 
    client_df.to_csv(os.path.join(output_dir, 'client.csv'), index=False)
    campaign_df.to_csv(os.path.join(output_dir, 'campaign.csv'), index=False)
    economics_df.to_csv(os.path.join(output_dir, 'economics.csv'), index=False)

    return


if __name__ == "__main__":
    clean_campaign_data()
