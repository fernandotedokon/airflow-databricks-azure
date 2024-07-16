# Databricks notebook source
import requests
from pyspark.sql.functions import lit

# COMMAND ----------

def extracting_data(date, base="BRL"):

    url = f"https://api.apilayer.com/exchangerates_data/{date}&base={base}"

    headers= {
    "apikey": "ubuA3TMzjO1klUsAx3CWqBhKCxvXpeW0"
    }

    parametros = {"base": base}

    response = requests.request("GET", url, headers=headers)

    if response.status_code != 200:
        raise Exception("Não consegui extrair dados!!!")

    return response.json()

# COMMAND ----------

def data_to_dataframe (dado_json): 
    data_tupla = [(moeda, float (taxa)) for moeda, taxa in dado_json["rates"].items()]
    return data_tupla

# COMMAND ----------

def save_file_parquet(conversoes_extraidas):
    ano, mes, dia = conversoes_extraidas['date'].split('-')
    caminho_completo = f"dbfs:/databricks-results/01_transformation_initial/{ano}/{mes}/{dia}"
    response = data_to_dataframe(conversoes_extraidas)
    df_conversoes = spark.createDataFrame(response, schema=['moeda', 'taxa'])
    df_conversoes = df_conversoes.withColumn("data", lit(f"{ano}-{mes}-{dia}"))

    df_conversoes.write.format("parquet")\
        .mode("overwrite")\
        .save(caminho_completo)
    
    print(f"Os arquivos foram salvos em {caminho_completo}")
