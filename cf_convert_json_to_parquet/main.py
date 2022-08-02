import pandas as pd
from parametros.env_configs import EnvConfigs
from google.cloud import bigquery
from google.cloud import storage
import numpy as np
import json
from datetime import datetime
import logging
import google.cloud.logging

logging_client = google.cloud.logging.Client()
logging_client.get_default_handler()
logging_client.setup_logging()


def get_schema_bq(tabela):
    bq = bigquery.Client()
    tb_schema = bq.get_table(tabela).schema
    schema = list(map(lambda x: {"mode": x.mode, "name": x.name, "type": x.field_type}, tb_schema))
    for i in range(len(schema)):
        if schema[i]['name'] == 'DataIngestao':
            del schema[i]
            break
    return schema


def match_schema_file(schema_bq, dataframe):
    lista_colunas = list(map(lambda x: x['name'], schema_bq))

    columns = dataframe.columns
    # columns = columns.str

    colunas_faltantes = list(set(lista_colunas) - set(columns))
    colunas_adicionadas = list(set(columns) - set(lista_colunas))
    dict_log = {"colunas_faltantes": colunas_faltantes,
                "colunas_adicionadas": colunas_adicionadas,
                "schema_diferente": (len(colunas_faltantes) + len(colunas_adicionadas))}

    return dict_log


def schema_pandas_to_bq(schema):
    x = ""
    for value in schema:
        x = x + '"' + value["name"] + '": "' + value["type"].lower().replace('integer', 'int').replace('string', 'str').replace('timestamp', 'datetime64[ns]') + '", '

    r = "{" + x[:-2] + "}"

    d = json.loads(r)

    return d


def delete_blob(bucket_name, blob_name):
    """Deletes a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()

    print(f"Blob {blob_name} deleted.")


def main(event, context):

    uri = ""

    try:

        file = event

        uri = 'gs://' + file['bucket'] + "/" + file['name']

        logging.info('Processando o arquivo "{}".'.format(uri))

        logging.info('Arquivo : {}'.format(uri))

        env_configs = EnvConfigs()

        blob_path = file['name'].split("/")

        project = env_configs.get_gcp_project()
        bucket_destination = env_configs.get_bucket_destination()
        dataset = blob_path[0].replace('-', '_')
        table = blob_path[1]
        data_execucao_portfolio = blob_path[2]
        filename = blob_path[3]

        table_id = project + "." + dataset + "." + table

        logging.info('Interface : {}'.format(table))
        logging.info('DataExecucaoPortfolio : {}'.format(data_execucao_portfolio))

        bucket = file['bucket']
        file_path = file['name']
        time_created = file['timeCreated']

        logging.info('Obtendo o schema da tabela "{}".'.format(table_id))
        bq_schema = get_schema_bq(table_id)

        logging.info('Convertendo o schema da tabela "{}" para o formato Pandas.'.format(table_id))
        pd_schema = schema_pandas_to_bq(bq_schema)

        logging.info('Carregando o arquivo "{}" no dataframe.'.format(uri))
        df = pd.read_json(uri, lines=True, dtype=pd_schema)
        df_count = len(df.index)

        df = df.fillna(value=np.nan)

        df = df.replace('None', '')

        df["FilenameInput"] = uri

        logging.info('Verificando o schema da tabela "{}" e do arquivo "{}".'.format(table_id, uri))
        match_schema = match_schema_file(bq_schema, df)

        if match_schema["schema_diferente"] != 0:
            logging.error('O schema da tabela "{}" e do arquivo "{}" estão diferentes.'.format(table_id, uri))
            return {"status": "error", "details": "O schema do arquivo e da tabela estão diferentes.", "extra": match_schema}, 400

        path_destination = "gs://" + bucket_destination + "/" + dataset.replace('_', '-') + "/" + \
                           table + "/" + data_execucao_portfolio + "/" + filename + '.parquet.gzip'

        logging.info('Convertendo e salvando o arquivo "{}" no formato parquet em "{}".'.format(uri, path_destination))
        df.to_parquet(path_destination, compression='gzip')

        logging.info('Deletando o arquivo "{}".'.format(uri))
        delete_blob(bucket, file_path)

        logging.info('Registrando na tabela de controle.')
        current_date = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        rows_to_insert = [{u"NomeArquivo": uri, u"QuantidadeRegistro": df_count, u"OrigemBucket": bucket,
                           u"TimeCreated": time_created, u"LoadDatetime": current_date}]

        bq = bigquery.Client()
        bq.insert_rows_json('generali-datalake-dev.data_quality.controle', rows_to_insert)

    except FileNotFoundError:
        logging.error('Arquivo não encontrado "{}"'.format(uri))
        return {"status": "error", "details": "Arquivo " + uri + " não encontrado."}, 400

    except Exception as e:
        logging.exception(e)
        return {"status": "error", "details": str(type(e).__name__)}, 400

    logging.info('status : Success')
    return {"status": "success"}
