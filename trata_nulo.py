list_path = dbutils.fs.ls('dbfs:/FileStore/data/')
dbutils.fs.mkdirs('dbfs:/FileStore/data_tratados/')

for path in list_path:
    df_name = path[1]
    df = spark.read.csv(path[0], inferSchema=True, header=True)
    for coluna in df.columns:
        df = df.withColumn(coluna, F.expr(f"replace({coluna}, '\\\\N', '')"))   
    #df.write.csv(f'dbfs:/FileStore/data_tratados/{df_name}',header = True, sep = ';')
    #df.write.option("header", "true").option("delimiter", ";").mode("overwrite").csv(f'dbfs:/FileStore/data_tratados/{df_name}')
    df.write.mode("overwrite").format("csv").option("header", "true").option("delimiter", ";").save(f'dbfs:/FileStore/data_tratados/{df_name}')
