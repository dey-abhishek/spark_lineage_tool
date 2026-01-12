#!/bin/bash
# Beeline and multiple Hive operations
# Tests: Beeline execution, multiple SQL files, error handling

set -e

HIVE_SERVER="jdbc:hive2://hiveserver:10000/default"
USERNAME="dataeng"
RUN_DATE=$1

echo "Executing Hive scripts via Beeline for ${RUN_DATE}"

# Execute inline SQL
beeline -u ${HIVE_SERVER} -n ${USERNAME} -e "
    MSCK REPAIR TABLE prod.transactions;
    
    SELECT COUNT(*) as record_count 
    FROM prod.transactions 
    WHERE date = '${RUN_DATE}';
"

# Execute from multiple files
for sql_file in /sql/etl/*.hql; do
    echo "Executing $(basename ${sql_file})..."
    
    beeline -u ${HIVE_SERVER} \
        -n ${USERNAME} \
        -f ${sql_file} \
        --hivevar run_date=${RUN_DATE} \
        --hivevar env=prod
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to execute $(basename ${sql_file})"
        exit 1
    fi
done

# Export to HDFS
hive -e "
    INSERT OVERWRITE DIRECTORY '/data/exports/${RUN_DATE}'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    SELECT * FROM analytics.daily_summary
    WHERE date = '${RUN_DATE}';
"

echo "Beeline operations completed"

