from google.cloud import bigquery
from parametros.env_configs import EnvConfigs
import pandas as pd
import logging
import google.cloud.logging

logging_client = google.cloud.logging.Client()
logging_client.get_default_handler()
logging_client.setup_logging()


def main(event, context):

    uri = ""

    try:

        file = event

        uri = 'gs://' + file['bucket'] + "/" + file['name']

        logging.info('Processando o arquivo "{}".'.format(uri))

        env_configs = EnvConfigs()

        blob_path = file['name'].split("/")

        project = env_configs.get_bq_destination_project()
        dataset = blob_path[0].replace('-', '_')
        table = blob_path[1]
        data_execucao_portfolio = blob_path[2]
        # filename = blob_path[3]
        table_id = project + "." + dataset + "." + table

        logging.info('Interface : {}'.format(table))
        logging.info('DataExecucaoPortfolio : {}'.format(data_execucao_portfolio))

        logging.info('Carregando o arquivo "{}" no dataframe.'.format(uri))
        df = pd.read_parquet(uri)

        df["DataIngestao"] = pd.Timestamp.today()

        logging.info('Carregando o dataframe na tabela "{}".'.format(table_id))

        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig(
            create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
            source_format=bigquery.SourceFormat.PARQUET  # New config
        )

        load_job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )

        load_job.result()

    except FileNotFoundError:
        logging.error('Arquivo não encontrado "{}"'.format(uri))
        return {"status": "error", "details": "Arquivo " + uri + " não encontrado."}, 400

    except Exception as e:
        logging.exception(e)
        return {"status": "error", "details": str(type(e).__name__)}, 400

    logging.info('status : success')
    return {"status": "success"}
