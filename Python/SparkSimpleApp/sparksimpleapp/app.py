def rdd_csv_col_count(rdd):
    rdd1 = rdd.map(lambda s: (len(s.split(",")), ) )
    return rdd1