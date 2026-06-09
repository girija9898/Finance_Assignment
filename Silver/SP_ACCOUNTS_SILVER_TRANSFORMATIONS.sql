CREATE OR REPLACE PROCEDURE SILVER.UTILS.SP_ACCOUNTS_SILVER_TRANSFORMATIONS
(
    P_JOB_ID STRING
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import *
from snowflake.snowpark.window import Window

def run(session, p_job_id):

    try:

        # READ STREAM
        session.sql("""
            CREATE OR REPLACE TABLE SILVER.UTILS.TEMP_BRONZE_ACCOUNTS AS
            SELECT * FROM BRONZE.FINANCE.BRONZE_ACCOUNTS_STREAM
        """).collect()

        df_bronze_accounts = session.table('SILVER.UTILS.TEMP_BRONZE_ACCOUNTS')
        
        # df_bronze_accounts = session.table('BRONZE.FINANCE.ACCOUNTS')
        
        rows_processed = df_bronze_accounts.count()

        # ACCOUNT_TYPE STANDARDIZATION
        df_bronze_accounts = df_bronze_accounts.with_column(
            "ACCOUNT_TYPE",
            upper(trim(col("ACCOUNT_TYPE")))
        )

        df_bronze_accounts = df_bronze_accounts.with_column(
            "ACCOUNT_TYPE",
            when(
                col("ACCOUNT_TYPE").isin(
                    "SAVINGS",
                    "CURRENT",
                    "INVESTMENT",
                    "RETIREMENT",
                    "LOAN"
                ),
                col("ACCOUNT_TYPE")
            ).otherwise(lit("UNKNOWN"))
        )

        # ACCOUNT_STATUS STANDARDIZATION
        df_bronze_accounts = df_bronze_accounts.with_column(
            "ACCOUNT_STATUS",
            upper(trim(col("ACCOUNT_STATUS")))
        )

        df_bronze_accounts = df_bronze_accounts.with_column(
            "ACCOUNT_STATUS",
            when(
                col("ACCOUNT_STATUS").isin(
                    "ACTIVE",
                    "INACTIVE",
                    "CLOSED",
                    "SUSPENDED"
                ),
                col("ACCOUNT_STATUS")
            ).otherwise(lit("UNKNOWN"))
        )

        # BASE_CURRENCY STANDARDIZATION
        df_bronze_accounts = df_bronze_accounts.with_column(
            "BASE_CURRENCY",
            upper(trim(col("BASE_CURRENCY")))
        )

        # CURRENCY VALIDATION
        currency_regex = '^[A-Z]{3}$'

        df_bronze_accounts = df_bronze_accounts.with_column(
            "CURRENCY_VALID",
            regexp_like(
                col("BASE_CURRENCY"),
                lit(currency_regex)
            )
        )

        # CLOSED_DATE VALIDATION
        df_bronze_accounts = df_bronze_accounts.with_column(
            "DATE_VALID",
            when(
                col("CLOSED_DATE").is_null(),
                lit(True)
            ).otherwise(
                col("CLOSED_DATE") >= col("OPENED_DATE")
            )
        )

        # HANDLE MISSING ADVISOR_CODE
        df_bronze_accounts = df_bronze_accounts.with_column(
            "ADVISOR_CODE",
            when(
                col("ADVISOR_CODE").is_null()
                |
                (trim(col("ADVISOR_CODE")) == ''),
                lit("UNASSIGNED")
            ).otherwise(col("ADVISOR_CODE"))
        )

        # ACCOUNT_NUMBER VALIDATION
        df_bronze_accounts = df_bronze_accounts.with_column(
            "ACCOUNT_NUMBER_VALID",
            when(
                col("ACCOUNT_NUMBER").is_null()
                |
                (trim(col("ACCOUNT_NUMBER")) == ''),
                lit(False)
            ).otherwise(lit(True))
        )

        # RECORD HASH
        df_bronze_accounts = df_bronze_accounts.with_column(
            "RECORD_HASH_SILVER",
            md5(concat_ws(lit("|"), col("ACCOUNT_ID"), col("CREATED_AT"), col("UPDATED_AT")))
        )

        # ADD AUDIT_JOB_ID
        df_bronze_accounts = df_bronze_accounts.with_column(
            "AUDIT_JOB_ID",
            lit(p_job_id)
        )

        # DEDUPLICATION
        window_spec = Window.partition_by(
            col("ACCOUNT_NUMBER")
        ).order_by(
            col("UPDATED_AT").desc()
        )

        df_bronze_accounts = df_bronze_accounts.with_column(
            "RN",
            row_number().over(window_spec)
        )

        df_bronze_accounts = df_bronze_accounts.filter(
            col("RN") == 1
        )

        # DQ STATUS
        df_bronze_accounts = df_bronze_accounts.with_column(
            "DQ_STATUS",
            when(
                col("CURRENCY_VALID")
                &
                col("DATE_VALID")
                &
                col("ACCOUNT_NUMBER_VALID"),
                lit("VALID")
            ).otherwise(lit("REJECTED"))
        )

        # DQ ERROR MESSAGE
        df_bronze_accounts = df_bronze_accounts.with_column(
            "DQ_ERROR_MESSAGE",
            when(
                ~col("CURRENCY_VALID"),
                lit("INVALID_BASE_CURRENCY")
            )
            .when(
                ~col("DATE_VALID"),
                lit("INVALID_CLOSED_DATE")
            )
            .when(
                ~col("ACCOUNT_NUMBER_VALID"),
                lit("MISSING_ACCOUNT_NUMBER")
            )
            .otherwise(lit(None))
        )

        # SPLIT VALID / REJECTED
        valid_df_bronze_accounts = df_bronze_accounts.filter(
            col("DQ_STATUS") == "VALID"
        )

        rejected_df_bronze_accounts = df_bronze_accounts.filter(
            col("DQ_STATUS") == "REJECTED"
        )

        # LOAD SILVER
        valid_df_bronze_accounts.select(
            col("ACCOUNT_ID"),
            col("CUSTOMER_ID"),
            col("ACCOUNT_NUMBER"),
            col("ACCOUNT_TYPE"),
            col("ACCOUNT_STATUS"),
            col("BASE_CURRENCY"),
            col("OPENED_DATE"),
            col("CLOSED_DATE"),
            col("ADVISOR_CODE"),
            col("CREATED_AT"),
            col("UPDATED_AT"),
            col("RECORD_HASH_SILVER").alias("RECORD_HASH"),
            col("AUDIT_JOB_ID"),
            col("DQ_STATUS")
        ).write.save_as_table(
            table_name="SILVER.FINANCE.ACCOUNTS",
            mode="truncate",
            column_order="name"
        )

        # LOAD REJECTED
        rejected_df_bronze_accounts.select(
            col("ACCOUNT_ID"),
            col("CUSTOMER_ID"),
            col("ACCOUNT_NUMBER"),
            col("ACCOUNT_TYPE"),
            col("ACCOUNT_STATUS"),
            col("BASE_CURRENCY"),
            col("OPENED_DATE"),
            col("CLOSED_DATE"),
            col("ADVISOR_CODE"),
            col("AUDIT_JOB_ID"),
            col("DQ_ERROR_MESSAGE").alias("REJECTION_REASON")
        ).write.save_as_table(
            table_name="SILVER.FINANCE.ACCOUNTS_REJECTED",
            mode="append",
            column_order="name"
        )

        # COUNTS
        rows_inserted = valid_df_bronze_accounts.count()
        rows_rejected = rejected_df_bronze_accounts.count()

        session.sql("DROP TABLE IF EXISTS SILVER.UTILS.TEMP_BRONZE_ACCOUNTS").collect()

        # RETURN RESULT
        return {
            "STATUS": "SUCCESS",
            "ROWS_PROCESSED": rows_processed,
            "ROWS_INSERTED": rows_inserted,
            "ROWS_REJECTED": rows_rejected,
            "ERROR_MESSAGE": 'INVALID DATA, IF REJECTED'
        }

    except Exception as e:
        return {
            "STATUS": "FAILED",
            "ROWS_PROCESSED": 0,
            "ROWS_INSERTED": 0,
            "ROWS_REJECTED": 0,
            "ERROR_MESSAGE": str(e)
        }
$$;