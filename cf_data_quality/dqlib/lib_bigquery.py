from google.cloud import bigquery


def getRowCount(bqProject, bqDataset, bqTable, DataExecucaoPortfolio):
    bigqueryClient = bigquery.Client()
    sql = """SELECT COUNT(1) as recordCount FROM {}.{}.{} WHERE DATE(DataExecucaoPortfolio) = '{}'""".format(
        bqProject, bqDataset, bqTable, DataExecucaoPortfolio)
    job = bigqueryClient.query(sql)
    rows = job.result()
    rowCount = 0
    for row in rows:
        rowCount = row.recordCount

    #if rowCount == 0:
    #    rowCount = 1

    return(rowCount)


def getSql(bqProject, bqDataset, bqTable, columnList, rowCount, DataExecucaoPortfolio):
    colIdx = 0
    sql = ""
    prefix = ""
    for col in columnList:
        if colIdx != 0:
            prefix = "UNION ALL" + "\n"

        colName = """'{}'""".format(col)
        total = """{}""".format(rowCount)
        minVal = """MIN({})""".format(col)
        maxVal = """MAX({})""".format(col)
        nullVal = """COUNT(CASE WHEN {} IS NULL THEN 1 END)""".format(col)
        uniqVal = """COUNT(DISTINCT({}))""".format(col)
        selVal = """CONCAT(ROUND(SAFE_DIVIDE({},{}) * 100, 2), '%')""".format(uniqVal, rowCount)
        denVal = """CONCAT(ROUND(SAFE_DIVIDE({} - {},{}) * 100, 2), '%')""".format(rowCount, nullVal, rowCount)

        sql = sql + prefix + """SELECT
                                    DATE('{}') as DataExecucaoPortfolio,                            
                                    {} as ColumnName,
                                    {} AS Total,
                                    CAST({} as String) as MinValue,
                                    CAST({} as String) as MaxValue,
                                    CAST({} as String) as NullValues,
                                    CAST({} as String) as Cardinality,
                                    CAST({} as String) as Selectivity,
                                    CAST({} as String) as Density
                                FROM {}.{}.{}
                                WHERE DATE(DataExecucaoPortfolio) = '{}'""".format(
            DataExecucaoPortfolio, colName, total, minVal, maxVal, nullVal, uniqVal, selVal, denVal, bqProject,
            bqDataset, bqTable, DataExecucaoPortfolio)
        colIdx = colIdx + 1
    return(sql)


def runSql(bqProject, datasetDataQualityName, datasetName, dataTable, reportTable, sqlQuery):
    tableRef = """{}.{}.{}""".format(bqProject, datasetName, dataTable)
    reportRef = """{}.{}.{}""".format(bqProject, datasetDataQualityName, reportTable)
    bigqueryClient = bigquery.Client()
    sql = """INSERT INTO {} 
                SELECT current_timestamp, '{}' as TableRef, dq.* 
                FROM ({}) dq""".format(reportRef, tableRef, sqlQuery)

    sqlJob = bigqueryClient.query(sql)
    sqlJob.result()
    return 0