CREATE OR REPLACE PROCEDURE SILVER.UTILS.SP_CASH_TRANSACTIONS_SILVER_TRANSFORMATIONS
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
            CREATE OR REPLACE TABLE SILVER.UTILS.TEMP_BRONZE_CASH_TRANSACTIONS AS
            SELECT * FROM BRONZE.FINANCE.BRONZE_CASHTRANSACTIONS_STREAM
        """).collect()

        df_bronze_cash_transactions = session.table('SILVER.UTILS.TEMP_BRONZE_CASH_TRANSACTIONS')
        
        #df_bronze_cash_transactions = session.table('BRONZE.FINANCE.CASH_TRANSACTIONS')
		
        #df_bronze_cash_transactions = session.table(
        #    "BRONZE.FINANCE.CASH_TRANSACTIONS_STREAM"
        #)

        rows_processed = df_bronze_cash_transactions.count()

        # STANDARDIZE TRANSACTION_TYPE
        df_bronze_cash_transactions = df_bronze_cash_transactions.with_column(
            "TRANSACTION_TYPE",
            upper(trim(col("TRANSACTION_TYPE")))
        )

        df_bronze_cash_transactions = df_bronze_cash_transactions.with_column(
            "TRANSACTION_TYPE",
            when(
                col("TRANSACTION_TYPE").isin(
                    "DEPOSIT",
                    "WITHDRAWAL",
                    "DIVIDEND",
                    "FEE"
                ),
                col("TRANSACTION_TYPE")
            ).otherwise(lit("UNKNOWN"))
        )

        # STANDARDIZE TRANSACTION_STATUS
        df_bronze_cash_transactions = df_bronze_cash_transactions.with_column(
            "TRANSACTION_STATUS",
            upper(trim(col("TRANSACTION_STATUS")))
        )

        df_bronze_cash_transactions = df_bronze_cash_transactions.with_column(
            "TRANSACTION_STATUS",
            when(
                col("TRANSACTION_STATUS").isin(
                    "SUCCESS",
                    "FAILED",
                    "PENDING",
                    "CANCELLED"
                ),
                col("TRANSACTION_STATUS")
            ).otherwise(lit("INVALID"))
        )

        # STANDARDIZE CURRENCY
        df_bronze_cash_transactions = df_bronze_cash_transactions.with_column(
            "CURRENCY",
            upper(trim(col("CURRENCY")))
        )

        # VALIDATE CURRENCY
        currency_regex = '^[A-Z]{3}$'

        df_bronze_cash_transactions = df_bronze_cash_transactions.with_column(
            "CURRENCY_VALID",
            regexp_like(
                col("CURRENCY"),
                lit(currency_regex)
            )
        )

        # HANDLE NULL FEE_AMOUNT
        df_bronze_cash_transactions = df_bronze_cash_transactions.with_column(
            "FEE_AMOUNT",
            when(
                col("FEE_AMOUNT").is_null(),
                lit(0)
            ).otherwise(col("FEE_AMOUNT"))
        )

        # VALIDATE AMOUNT
        df_bronze_cash_transactions = df_bronze_cash_transactions.with_column(
            "AMOUNT_VALID",
            when(
                col("AMOUNT").is_null()
                |
                (col("AMOUNT") <= 0),
                lit(False)
            ).otherwise(lit(True))
        )

        # VALIDATE TRANSACTION_STATUS
        df_bronze_cash_transactions = df_bronze_cash_transactions.with_column(
            "STATUS_VALID",
            when(
                col("TRANSACTION_STATUS") == "INVALID",
                lit(False)
            ).otherwise(lit(True))
        )

        # VALIDATE REFERENCE_NUMBER
        df_bronze_cash_transactions = df_bronze_cash_transactions.with_column(
            "REFERENCE_VALID",
            when(
                col("REFERENCE_NUMBER").is_null()
                |
                (trim(col("REFERENCE_NUMBER")) == ''),
                lit(False)
            ).otherwise(lit(True))
        )

        # RECORD HASH
        df_bronze_cash_transactions = df_bronze_cash_transactions.with_column(
            "RECORD_HASH",
			md5(concat_ws(lit("|"), col("CASH_TRANSACTION_ID"), col("CREATED_AT"), col("UPDATED_AT")))
        )

        # ADD AUDIT_JOB_ID
        df_bronze_cash_transactions = df_bronze_cash_transactions.with_column(
            "AUDIT_JOB_ID",
            lit(p_job_id)
        )

        # DEDUPLICATION
        window_spec = Window.partition_by(
            col("REFERENCE_NUMBER")
        ).order_by(
            col("UPDATED_AT").desc()
        )

        df_bronze_cash_transactions = df_bronze_cash_transactions.with_column(
            "RN",
            row_number().over(window_spec)
        )

        df_bronze_cash_transactions = df_bronze_cash_transactions.filter(
            col("RN") == 1
        )

        # DQ STATUS
        df_bronze_cash_transactions = df_bronze_cash_transactions.with_column(
            "DQ_STATUS",
            when(
                col("CURRENCY_VALID")
                &
                col("AMOUNT_VALID")
                &
                col("STATUS_VALID")
                &
                col("REFERENCE_VALID"),
                lit("VALID")
            ).otherwise(lit("REJECTED"))
        )

        # DQ ERROR MESSAGE
        df_bronze_cash_transactions = df_bronze_cash_transactions.with_column(
            "DQ_ERROR_MESSAGE",
            when(
                ~col("CURRENCY_VALID"),
                lit("INVALID_CURRENCY")
            )
            .when(
                ~col("AMOUNT_VALID"),
                lit("INVALID_AMOUNT")
            )
            .when(
                ~col("STATUS_VALID"),
                lit("INVALID_TRANSACTION_STATUS")
            )
            .when(
                ~col("REFERENCE_VALID"),
                lit("MISSING_REFERENCE_NUMBER")
            )
            .otherwise(lit(None))
        )

        # SPLIT VALID / REJECTED
        valid_df_bronze_cash_transactions = df_bronze_cash_transactions.filter(
            col("DQ_STATUS") == "VALID"
        )

        rejected_df_bronze_cash_transactions = df_bronze_cash_transactions.filter(
            col("DQ_STATUS") == "REJECTED"
        )

        # LOAD SILVER
        valid_df_bronze_cash_transactions.select(
            col("CASH_TRANSACTION_ID"),
            col("ACCOUNT_ID"),
            col("TRANSACTION_DATE"),
            col("TRANSACTION_TYPE"),
            col("AMOUNT"),
            col("CURRENCY"),
            col("REFERENCE_NUMBER"),
            col("TRANSACTION_STATUS"),
            col("FEE_AMOUNT"),
            col("CREATED_AT"),
            col("UPDATED_AT"),
            col("RECORD_HASH"),
            col("AUDIT_JOB_ID"),
            col("DQ_STATUS")
        ).write.save_as_table(
            table_name="SILVER.FINANCE.CASH_TRANSACTIONS",
            mode="truncate",
            column_order="name"
        )

        # LOAD REJECTED
        rejected_df_bronze_cash_transactions.select(
            col("CASH_TRANSACTION_ID"),
            col("ACCOUNT_ID"),
            col("TRANSACTION_DATE"),
            col("TRANSACTION_TYPE"),
            col("AMOUNT"),
            col("CURRENCY"),
            col("REFERENCE_NUMBER"),
            col("TRANSACTION_STATUS"),
            col("FEE_AMOUNT"),
            col("CREATED_AT"),
            col("UPDATED_AT"),
            col("AUDIT_JOB_ID"),
			col("DQ_ERROR_MESSAGE").alias("REJECTION_REASON")
        ).write.save_as_table(
            table_name="SILVER.FINANCE.CASH_TRANSACTIONS_REJECTED",
            mode="append",
            column_order="name"
        )

        # COUNTS
        rows_inserted = valid_df_bronze_cash_transactions.count()
        rows_rejected = rejected_df_bronze_cash_transactions.count()

        # RETURN RESULT
        return {
            "STATUS": "SUCCESS",
            "ROWS_PROCESSED": rows_processed,
            "ROWS_INSERTED": rows_inserted,
            "ROWS_REJECTED": rows_rejected,
            "ERROR_MESSAGE": 'INVALID FORMAT, IF REJECTED'
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