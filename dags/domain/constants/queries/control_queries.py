DAG_CONTROL_TABLE_INSERT = """
INSERT INTO ctr_control_transaction (
    CTTR_NM,
    CTTR_ST,
    CTTR_HR_DT_START_PROCESS,
    CTTR_DT_INSERT,
    CTTR_USER_INSERT
)
VALUES(
    '{dag_name}',
    {status},
    '{process_start_datetime}',
    '{insertion_datetime}',
    '{insertion_user}'
)
RETURNING CTTR_ID
"""

DAG_CONTROL_TABLE_UPDATE = """
UPDATE ctr_control_transaction
SET
    CTTR_ST = {status},
    CTTR_HR_DT_END_PROCESS = '{process_end_datetime}',
    CTTR_QT_PROCESSED_RECORD = {processed_records},
    CTTR_DT_UPDATE = '{update_datetime}',
    CTTR_USER_UPDATE = '{update_user}'
WHERE
    CTTR_ID = {dag_control_id}
"""

DAG_CONTROL_TABLE_SELECT_LAST_ID = """
SELECT
    CTTR_ID
FROM
    ctr_control_transaction
ORDER BY
    CTTR_ID DESC
LIMIT 1
"""

TASK_CONTROL_TABLE_INSERT = """
INSERT INTO ctr_control_transaction_detail (
    CTTR_ID,
    CTTD_NM,
    CTTD_ST,
    CTTD_HR_DT_START_TRANSACTION,
    CTTD_DT_INSERT,
    CTTD_USER_INSERT
)
VALUES(
    {dag_control_id},
    '{task_name}',
    {status},
    '{transaction_start_datetime}',
    '{insertion_datetime}',
    '{insertion_user}'
)
RETURNING CTTR_ID
"""

TASK_CONTROL_TABLE_UPDATE = """
UPDATE ctr_control_transaction_detail
SET
    CTTD_ST = {status},
    CTTD_HR_DT_END_TRANSACTION = '{transaction_end_datetime}',
    CTTD_QT_PROCESSED_RECORD = {processed_records},
    CTTD_DT_UPDATE = '{update_datetime}',
    CTTD_USER_UPDATE = '{update_user}'
WHERE
    CTTD_ID = {task_control_id}
"""

TASK_CONTROL_TABLE_SELECT_LAST_ID = """
SELECT
    CTTD_ID
FROM
    ctr_control_transaction_detail
ORDER BY
    CTTD_ID DESC
LIMIT 1
"""

TASK_ERROR_TABLE_INSERT = """
INSERT INTO ctr_control_transaction_error (
    CTTD_ID,
    CTTE_DS,
    CTTE_DT_INSERT,
    CTTE_USER_INSERT
)
VALUES(
    {task_id},
    '{error_details}',
    '{insertion_datetime}',
    '{insertion_user}'
)
"""
