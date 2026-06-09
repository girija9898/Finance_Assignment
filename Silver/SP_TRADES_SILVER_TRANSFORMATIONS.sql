CREATE OR REPLACE PROCEDURE SILVER.UTILS.SP_TRADES_SILVER_TRANSFORMATIONS
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
            CREATE OR REPLACE TABLE SILVER.UTILS.TEMP_BRONZE_TRADES AS
           SELECT * FROM BRONZE.FINANCE.BRONZE_TRADES_STREAM
        """).collect()

        df_bronze_trades = session.table('SILVER.UTILS.TEMP_BRONZE_TRADES')
        
        #df_bronze_trades = session.table('BRONZE.FINANCE.TRADES')

        rows_processed = df_bronze_trades.count()

        # STANDARDIZE TRADE_TYPE
        df_bronze_trades = df_bronze_trades.with_column(
            "TRADE_TYPE",
            upper(trim(col("TRADE_TYPE")))
        )
        df_bronze_trades = df_bronze_trades.with_column(
            "TRADE_TYPE",
            when(
                col("TRADE_TYPE").isin(
                    "BUY",
                    "SELL"
                ),
                col("TRADE_TYPE")
            ).otherwise(lit("UNKNOWN"))
        )

        # STANDARDIZE TRADE_STATUS
        df_bronze_trades = df_bronze_trades.with_column(
            "TRADE_STATUS",
            upper(trim(col("TRADE_STATUS")))
        )
        df_bronze_trades = df_bronze_trades.with_column(
            "TRADE_STATUS",
            when(
                col("TRADE_STATUS").isin(
                    "COMPLETED",
                    "PENDING",
                    "FAILED",
                    "CANCELLED"
                ),
                col("TRADE_STATUS")
            ).otherwise(lit("INVALID"))
        )

        # STANDARDIZE TRADE_CURRENCY
        df_bronze_trades = df_bronze_trades.with_column(
            "TRADE_CURRENCY",
            upper(trim(col("TRADE_CURRENCY")))
        )

        # VALIDATE CURRENCY
        currency_regex = '^[A-Z]{3}$'
        df_bronze_trades = df_bronze_trades.with_column(
            "CURRENCY_VALID",
            regexp_like(
                col("TRADE_CURRENCY"),
                lit(currency_regex)
            )
        )

        # VALIDATE TRADE_TYPE
        df_bronze_trades = df_bronze_trades.with_column(
            "TRADE_TYPE_VALID",
            when(
                col("TRADE_TYPE") == "UNKNOWN",
                lit(False)
            ).otherwise(lit(True))
        )

        # VALIDATE QUANTITY
        df_bronze_trades = df_bronze_trades.with_column(
            "QUANTITY_VALID",
            when(
                col("QUANTITY").is_null()
                |
                (col("QUANTITY") <= 0),
                lit(False)
            ).otherwise(lit(True))
        )

        # VALIDATE TRADE_PRICE
        df_bronze_trades = df_bronze_trades.with_column(
            "TRADE_PRICE_VALID",
            when(
                col("TRADE_PRICE").is_null()
                |
                (col("TRADE_PRICE") <= 0),
                lit(False)
            ).otherwise(lit(True))
        )

        # HANDLE NULL FEES/TAX/BROKERAGE
        df_bronze_trades = df_bronze_trades.with_column(
            "BROKERAGE_RATE",
            when(
                col("BROKERAGE_RATE").is_null(),
                lit(0)
            ).otherwise(col("BROKERAGE_RATE"))
        )
        df_bronze_trades = df_bronze_trades.with_column(
            "TAX_RATE",
            when(
                col("TAX_RATE").is_null(),
                lit(0)
            ).otherwise(col("TAX_RATE"))
        )
        df_bronze_trades = df_bronze_trades.with_column(
            "EXCHANGE_FEE",
            when(
                col("EXCHANGE_FEE").is_null(),
                lit(0)
            ).otherwise(col("EXCHANGE_FEE"))
        )

        # VALIDATE SETTLEMENT_DATE
        df_bronze_trades = df_bronze_trades.with_column(
            "SETTLEMENT_VALID",
            when(
                col("SETTLEMENT_DATE").is_null(),
                lit(False)
            ).otherwise(
                to_date(col("SETTLEMENT_DATE"))
                >=
                to_date(col("TRADE_DATE"))
            )
        )

        # VALIDATE ACCOUNT_ID
        df_bronze_trades = df_bronze_trades.with_column(
            "ACCOUNT_VALID",
            when(
                col("ACCOUNT_ID").is_null()
                |
                (trim(col("ACCOUNT_ID")) == ''),
                lit(False)
            ).otherwise(lit(True))
        )

        # VALIDATE SECURITY_ID
        df_bronze_trades = df_bronze_trades.with_column(
            "SECURITY_VALID",
            when(
                col("SECURITY_ID").is_null()
                |
                (trim(col("SECURITY_ID")) == ''),
                lit(False)
            ).otherwise(lit(True))
        )

        # VALIDATE TRADE_STATUS
        df_bronze_trades = df_bronze_trades.with_column(
            "STATUS_VALID",
            when(
                col("TRADE_STATUS") == "INVALID",
                lit(False)
            ).otherwise(lit(True))
        )

        # RECORD HASH
        df_bronze_trades = df_bronze_trades.with_column(
            "RECORD_HASH",
			md5(concat_ws(lit("|"), col("TRADE_ID"), col("CREATED_AT"), col("UPDATED_AT")))
        )

        # ADD AUDIT_JOB_ID
        df_bronze_trades = df_bronze_trades.with_column(
            "AUDIT_JOB_ID",
            lit(p_job_id)
        )

        # REMOVE DUPLICATES - KEEP LATEST UPDATED_AT
        window_spec = Window.partition_by(
            col("TRADE_ID")
        ).order_by(
            col("UPDATED_AT").desc()
        )
        df_bronze_trades = df_bronze_trades.with_column(
            "RN",
            row_number().over(window_spec)
        )
        df_bronze_trades = df_bronze_trades.filter(
            col("RN") == 1
        )

        # DQ STATUS
        df_bronze_trades = df_bronze_trades.with_column(
            "DQ_STATUS",
            when(
                col("TRADE_TYPE_VALID")
                &
                col("QUANTITY_VALID")
                &
                col("TRADE_PRICE_VALID")
                &
                col("SETTLEMENT_VALID")
                &
                col("CURRENCY_VALID")
                &
                col("ACCOUNT_VALID")
                &
                col("SECURITY_VALID")
                &
                col("STATUS_VALID"),
                lit("VALID")
            ).otherwise(lit("REJECTED"))
        )

        # DQ ERROR MESSAGE
        df_bronze_trades = df_bronze_trades.with_column(
            "DQ_ERROR_MESSAGE",
            when(
                ~col("TRADE_TYPE_VALID"),
                lit("INVALID_TRADE_TYPE")
            )
            .when(
                ~col("QUANTITY_VALID"),
                lit("INVALID_QUANTITY")
            )
            .when(
                ~col("TRADE_PRICE_VALID"),
                lit("INVALID_TRADE_PRICE")
            )
            .when(
                ~col("SETTLEMENT_VALID"),
                lit("INVALID_SETTLEMENT_DATE")
            )
            .when(
                ~col("CURRENCY_VALID"),
                lit("INVALID_TRADE_CURRENCY")
            )
            .when(
                ~col("ACCOUNT_VALID"),
                lit("MISSING_ACCOUNT_ID")
            )
            .when(
                ~col("SECURITY_VALID"),
                lit("MISSING_SECURITY_ID")
            )
            .when(
                ~col("STATUS_VALID"),
                lit("INVALID_TRADE_STATUS")
            )
            .otherwise(lit(None))
        )

        # SPLIT VALID / REJECTED
        valid_df_bronze_trades = df_bronze_trades.filter(
            col("DQ_STATUS") == "VALID"
        )

        rejected_df_bronze_trades = df_bronze_trades.filter(
            col("DQ_STATUS") == "REJECTED"
        )

        # LOAD SILVER
        valid_df_bronze_trades.select(
            col("TRADE_ID"),
            col("ACCOUNT_ID"),
            col("SECURITY_ID"),
            col("TRADE_DATE"),
            col("SETTLEMENT_DATE"),
            col("TRADE_TYPE"),
            col("QUANTITY"),
            col("TRADE_PRICE"),
            col("BROKERAGE_RATE"),
            col("TAX_RATE"),
            col("EXCHANGE_FEE"),
            col("TRADE_CURRENCY"),
            col("TRADE_STATUS"),
            col("CREATED_AT"),
            col("UPDATED_AT"),
            col("RECORD_HASH"),
            col("AUDIT_JOB_ID"),
            col("DQ_STATUS")
        ).write.save_as_table(
            table_name="SILVER.FINANCE.TRADES",
            mode="truncate",
            column_order="name"
        )

        # LOAD REJECTED
        rejected_df_bronze_trades.select(
            col("TRADE_ID"),
            col("ACCOUNT_ID"),
            col("SECURITY_ID"),
            col("TRADE_DATE"),
            col("SETTLEMENT_DATE"),
            col("TRADE_TYPE"),
            col("QUANTITY"),
            col("TRADE_PRICE"),
            col("BROKERAGE_RATE"),
            col("TAX_RATE"),
            col("EXCHANGE_FEE"),
            col("TRADE_CURRENCY"),
            col("TRADE_STATUS"),
            col("CREATED_AT"),
            col("UPDATED_AT"),
            col("AUDIT_JOB_ID"),
            col("DQ_ERROR_MESSAGE").alias("REJECTION_REASON")
        ).write.save_as_table(
            table_name="SILVER.FINANCE.TRADES_REJECTED",
            mode="append",
            column_order="name"
        )

        # COUNTS
        rows_inserted = valid_df_bronze_trades.count()
        rows_rejected = rejected_df_bronze_trades.count()

        # RETURN RESULT
        return {
            "STATUS": "SUCCESS",
            "ROWS_PROCESSED": rows_processed,
            "ROWS_INSERTED": rows_inserted,
            "ROWS_REJECTED": rows_rejected,
            "ERROR_MESSAGE": 'IInvalid Format, if rows rejected'
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