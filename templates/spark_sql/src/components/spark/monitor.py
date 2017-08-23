
def log_jobgroup(spark_context, log_id, meta_str):
    """
    set identifier and string for job log
    """
    log_id = log_id + 1
    spark_context.setJobGroup(str(log_id), meta_str)
    return log_id
