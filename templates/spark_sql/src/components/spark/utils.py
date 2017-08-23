MODE_SPARK_LOCAL        = 'local'
MODE_SPARK_STANDALONE   = 'spark_standalone'
MODE_SPARK_CLUSTER      = 'spark_cluster'

# cluster/standalone or local mode
def acquire_url(mode_operation):
    """
    Acquire url for spark master
    Modes supported: Local, Spark Standalone
    """
    url = None
    if mode_operation   == MODE_SPARK_LOCAL: url = "local[*]"
    elif mode_operation == MODE_SPARK_STANDALONE: url = "spark://master:7077"
    return url
