CREATE OR REPLACE PROCEDURE SILVER.UTILS.SP_MARKET_PRICES_SILVER_TRANSFORMATIONS
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
            CREATE OR REPLACE TABLE SILVER.UTILS.TEMP_BRONZE_MARKET_PRICES AS
            SELECT * FROM BRONZE.FINANCE.BRONZE_MARKETPRICES_STREAM
        """).collect()

        df_bronze_market_prices = session.table('SILVER.UTILS.TEMP_BRONZE_MARKET_PRICES')
        
        #df_bronze_market_prices = session.table('BRONZE.FINANCE.MARKET_PRICES')

        rows_processed = df_bronze_market_prices.count()

        # STANDARDIZE PRICE_CURRENCY
        df_bronze_market_prices = df_bronze_market_prices.with_column(
            "PRICE_CURRENCY",
            upper(trim(col("PRICE_CURRENCY")))
        )

        # VALIDATE CURRENCY
        currency_regex = '^[A-Z]{3}$'
        df_bronze_market_prices = df_bronze_market_prices.with_column(
            "CURRENCY_VALID",
            regexp_like(
                col("PRICE_CURRENCY"),
                lit(currency_regex)
            )
        )

        # HANDLE NULL ADJUSTED_CLOSE_PRICE
        df_bronze_market_prices = df_bronze_market_prices.with_column(
            "ADJUSTED_CLOSE_PRICE",
            when(
                col("ADJUSTED_CLOSE_PRICE").is_null(),
                col("CLOSE_PRICE")
            ).otherwise(col("ADJUSTED_CLOSE_PRICE"))
        )

        # VALIDATE HIGH_PRICE >= LOW_PRICE
        df_bronze_market_prices = df_bronze_market_prices.with_column(
            "PRICE_RANGE_VALID",
            when(
                col("HIGH_PRICE") >= col("LOW_PRICE"),
                lit(True)
            ).otherwise(lit(False))
        )

        # VALIDATE POSITIVE PRICE VALUES
        df_bronze_market_prices = df_bronze_market_prices.with_column(
            "PRICE_VALUE_VALID",
            when(
                (col("OPEN_PRICE") > 0)
                &
                (col("HIGH_PRICE") > 0)
                &
                (col("LOW_PRICE") > 0)
                &
                (col("CLOSE_PRICE") > 0)
                &
                (col("ADJUSTED_CLOSE_PRICE") > 0),
                lit(True)
            ).otherwise(lit(False))
        )

        # VALIDATE FX_RATE_TO_BASE
        df_bronze_market_prices = df_bronze_market_prices.with_column(
            "FX_RATE_VALID",
            when(
                col("FX_RATE_TO_BASE").is_null()
                |
                (col("FX_RATE_TO_BASE") <= 0),
                lit(False)
            ).otherwise(lit(True))
        )

        # VALIDATE SECURITY_ID
        df_bronze_market_prices = df_bronze_market_prices.with_column(
            "SECURITY_VALID",
            when(
                col("SECURITY_ID").is_null()
                |
                (trim(col("SECURITY_ID")) == ''),
                lit(False)
            ).otherwise(lit(True))
        )

        # VALIDATE PRICE_DATE
        df_bronze_market_prices = df_bronze_market_prices.with_column(
            "PRICE_DATE_VALID",
            when(
                col("PRICE_DATE").is_null(),
                lit(False)
            ).otherwise(lit(True))
        )

        # RECORD HASH
        df_bronze_market_prices = df_bronze_market_prices.with_column(
            "RECORD_HASH",
			md5(concat_ws(lit("|"), col("SECURITY_ID"), col("CREATED_AT"), col("UPDATED_AT")))
        )

        # ADD AUDIT_JOB_ID
        df_bronze_market_prices = df_bronze_market_prices.with_column(
            "AUDIT_JOB_ID",
            lit(p_job_id)
        )

        # REMOVE DUPLICATES - KEEP LATEST UPDATED_AT
        window_spec = Window.partition_by(
            col("SECURITY_ID"),
            col("PRICE_DATE")
        ).order_by(
            col("UPDATED_AT").desc()
        )

        df_bronze_market_prices = df_bronze_market_prices.with_column(
            "RN",
            row_number().over(window_spec)
        )

        df_bronze_market_prices = df_bronze_market_prices.filter(
            col("RN") == 1
        )

        # DQ STATUS
        df_bronze_market_prices = df_bronze_market_prices.with_column(
            "DQ_STATUS",
            when(
                col("CURRENCY_VALID")
                &
                col("PRICE_RANGE_VALID")
                &
                col("PRICE_VALUE_VALID")
                &
                col("FX_RATE_VALID")
                &
                col("SECURITY_VALID")
                &
                col("PRICE_DATE_VALID"),
                lit("VALID")
            ).otherwise(lit("REJECTED"))
        )

        # DQ ERROR MESSAGE
        df_bronze_market_prices = df_bronze_market_prices.with_column(
            "DQ_ERROR_MESSAGE",
            when(
                ~col("CURRENCY_VALID"),
                lit("INVALID_PRICE_CURRENCY")
            )
            .when(
                ~col("PRICE_RANGE_VALID"),
                lit("HIGH_PRICE_LESS_THAN_LOW_PRICE")
            )
            .when(
                ~col("PRICE_VALUE_VALID"),
                lit("INVALID_PRICE_VALUE")
            )
            .when(
                ~col("FX_RATE_VALID"),
                lit("INVALID_FX_RATE")
            )
            .when(
                ~col("SECURITY_VALID"),
                lit("MISSING_SECURITY_ID")
            )
            .when(
                ~col("PRICE_DATE_VALID"),
                lit("MISSING_PRICE_DATE")
            )
            .otherwise(lit(None))
        )

        # SPLIT VALID / REJECTED
        valid_df_bronze_market_prices = df_bronze_market_prices.filter(
            col("DQ_STATUS") == "VALID"
        )

        rejected_df_bronze_market_prices = df_bronze_market_prices.filter(
            col("DQ_STATUS") == "REJECTED"
        )

        # LOAD SILVER
        valid_df_bronze_market_prices.select(
            col("PRICE_ID"),
            col("SECURITY_ID"),
            col("PRICE_DATE"),
            col("OPEN_PRICE"),
            col("HIGH_PRICE"),
            col("LOW_PRICE"),
            col("CLOSE_PRICE"),
            col("ADJUSTED_CLOSE_PRICE"),
            col("PRICE_CURRENCY"),
            col("FX_RATE_TO_BASE"),
            col("CREATED_AT"),
            col("UPDATED_AT"),
            col("RECORD_HASH"),
            col("AUDIT_JOB_ID"),
            col("DQ_STATUS")
        ).write.save_as_table(
            table_name="SILVER.FINANCE.MARKET_PRICES",
            mode="truncate",
            column_order="name"
        )

        # LOAD REJECTED
        rejected_df_bronze_market_prices.select(
            col("PRICE_ID"),
            col("SECURITY_ID"),
            col("PRICE_DATE"),
            col("OPEN_PRICE"),
            col("HIGH_PRICE"),
            col("LOW_PRICE"),
            col("CLOSE_PRICE"),
            col("ADJUSTED_CLOSE_PRICE"),
            col("PRICE_CURRENCY"),
            col("FX_RATE_TO_BASE"),
            col("CREATED_AT"),
            col("UPDATED_AT"),
            col("AUDIT_JOB_ID"),
            col("DQ_ERROR_MESSAGE").alias("REJECTION_REASON")
        ).write.save_as_table(
            table_name="SILVER.FINANCE.MARKET_PRICES_REJECTED",
            mode="append",
            column_order="name"
        )

        # COUNTS
        rows_inserted = valid_df_bronze_market_prices.count()
        rows_rejected = rejected_df_bronze_market_prices.count()

        # RETURN RESULT
        return {
            "STATUS": "SUCCESS",
            "ROWS_PROCESSED": rows_processed,
            "ROWS_INSERTED": rows_inserted,
            "ROWS_REJECTED": rows_rejected,
            "ERROR_MESSAGE": 'Invalid format, if there are rejected rows'
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