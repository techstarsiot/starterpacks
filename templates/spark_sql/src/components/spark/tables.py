import pandas as pd
import json

### Create Tables/Hive Metastore
def create_table(session, df_in, name, path_dir, cache_state=True):
    """
    creates a mapped table view of in memory table with appropriate cache
    """

    df_in.createOrReplaceTempView(name)                        #--- create table
    if cache_state: df_in.cache()                              #--- caches specified table in-memory
    #df_view = spark.table(name)

def create_hive_table(session, name, hive_name, path_dir, cache_state=True):
    """
    create mapped table in hive metastore
    """
    session.sql("DROP TABLE IF EXISTS {}".format(hive_name))     #--- remove existing same table in hive
    session.table(name) \
           .write.option("path", path_dir) \
           .saveAsTable(hive_name)                               #--- save in hive metastore to make queries
    if cache_state: session.catalog.cacheTable(hive_name)        #--- caches in hive metastore


def evict_cache(session, name, hive_name=None, evict_all=False):
    """
    remove mapped table(s) from in-memory cache
    note that there may not be an associated hive table mapped if not existing indicated
    """
    if evict_all:
        session.catalog.clearCache()                             #--- removes all cached tables
    else:
        if name: session.catalog.uncacheTable(table_name)        #--- uncache table individually
        if hive_name: session.catalog.uncacheTable(hive_name)    #--- uncache table individually

def get_info(session, name, hive_name):
    """
    acquire information about db, table metastore
    """
    df_dbs    = pd.DataFrame(session.catalog.listDatabases())
    df_tables = pd.DataFrame(session.catalog.listTables())
    table_cache_states = {'table_cache_state': session.catalog.isCached(name),
                          'hive_cache_state':  session.catalog.isCached(hive_name)}
    return df_dbs, df_tables, table_cache_states

def sql_query(session, query, table_name, agg=""):
    """
    perform sql query aggregations on standard table
    """
    df_query  = session.sql("{query} FROM {table} {agg}".format(query=query, table=table_name, agg=agg))
    return sql_query_reponse(df_query)

def sql_query_hive(session, query, hive_table_name, agg=""):
    """
    perform sql query aggregations on hive table
    """
    df_query = session.sql("{query} FROM {table} agg{}".format(query=query, table=hive_table_name, agg=agg))
    return sql_query_reponse(df_query)

def sql_query_reponse(df):
    """
    handle reponse form sql query to appropriate format
    """
    df_query_pandas = df.toPandas()
    df_query_json   = df_query_pandas.to_json(orient='records')
    df_query_json   = json.loads(df_query_json)
    return df_query_pandas, df_query_json
