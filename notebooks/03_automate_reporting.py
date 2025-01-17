# Databricks notebook source
import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import pyspark.pandas as ps


# COMMAND ----------

slack_token_v2 = "xxxx"
client = WebClient(token=slack_token_v2)


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-results/02_transformation_end/valores_reais/"))

# COMMAND ----------

nome_arquivo = dbutils.fs.ls("dbfs:/databricks-results/02_transformation_end/valores_reais/")[-1].name


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-results/02_transformation_end/valores_reais/"))

# COMMAND ----------

path = "/dbfs/databricks-results/02_transformation_end/valores_reais/" + nome_arquivo


# COMMAND ----------

try:
    enviando_arquivo_csv = client.files_upload_v2(
        channel="C07CG2SV2KZ",  
        title="Arquivo no formato CSV do valor do real convertido",
        file=path,
        filename="valores_reais.csv",
        initial_comment="Segue anexo o arquivo CSV:",
    )
except SlackApiError as e:
    # You will get a SlackApiError if "ok" is False
    assert e.response["ok"] is False
    assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
    print(f"Got an error: {e.response['error']}")

# COMMAND ----------

ps.read_csv("dbfs:/databricks-results/02_transformation_end/valores_reais/").head()

# COMMAND ----------

df_valores_reais = ps.read_csv("dbfs:/databricks-results/02_transformation_end/valores_reais/")


# COMMAND ----------

df_valores_reais.plot.line(x="data", y='USD')


# COMMAND ----------

df_valores_reais.plot.line(x="data", y='USD')

# COMMAND ----------

!mkdir imagens

# COMMAND ----------

for moeda in df_valores_reais.columns[1:]:
    fig = df_valores_reais.plot.line(x="data", y=moeda)
    fig.write_image(f"./imagens/{moeda}.png")

# COMMAND ----------

def enviando_imagens(moeda_cotacao):
    enviando_imagens = client.files_upload_v2(
        channel='C07CG2SV2KZ',
        title="Arquivo imagens de cotacoes",
        file=f"./imagens/{moeda_cotacao}.png"
    )


# COMMAND ----------

display(moeda)

# COMMAND ----------

for moeda in df_valores_reais.columns[1:]:
    enviando_imagens(moeda)
