

class Metrics_Queries:

    count_null = "SUM(CASE WHEN {0} IS NULL THEN 1 ELSE 0 END) as {1}_nulls"
    
    
    
