import yaml
from dqlib import lib_bigquery
from datetime import datetime, timedelta
import os


def main(request):
    
    data = request.get_json()

    DataExecucaoPortfolio = None

    if data and 'DataExecucaoPortfolio' in data:
        DataExecucaoPortfolio = data["DataExecucaoPortfolio"]
    else:
        DataExecucaoPortfolio = datetime.now() - timedelta(1)
        DataExecucaoPortfolio = DataExecucaoPortfolio.strftime('%Y-%m-%d')

    configFile = "config/dq.yaml"
    my_path = os.path.abspath(os.path.dirname(__file__))
    path = os.path.join(my_path, configFile)
    configList = yaml.safe_load(open(path))

    for tableConf in configList:
        project = tableConf['project']
        datasetName = tableConf['datasetName']
        datasetDataQualityName = tableConf['datasetDataQualityName']
        dataTable = tableConf['dataTable']
        reportTable = tableConf['reportTable']
        columnList = tableConf['includeColumns']

        # Get Row Count
        rowCount = lib_bigquery.getRowCount(project, datasetName, dataTable, DataExecucaoPortfolio)

        # Prepare Dynamic SQL Statement
        sqlQuery = lib_bigquery.getSql(project, datasetName, dataTable, columnList, rowCount, DataExecucaoPortfolio)

        # Run SQL & Display Output
        lib_bigquery.runSql(project, datasetDataQualityName, datasetName, dataTable, reportTable, sqlQuery)

    return {"status": "success"}
