CREATE OR REPLACE PROCEDURE
SILVER.UTILS.SP_SECURITIES_SILVER_TRANSFORMATIONS
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
            CREATE OR REPLACE TABLE SILVER.UTILS.TEMP_BRONZE_SECURITIES AS
            SELECT * FROM BRONZE.FINANCE.BRONZE_SECURITIES_STREAM
        """).collect()

        df_bronze_securities = session.table('SILVER.UTILS.TEMP_BRONZE_SECURITIES')
        
        #df_bronze_securities = session.table('BRONZE.FINANCE.SECURITIES')

        rows_processed = df_bronze_securities.count()

        # STANDARDIZE SECURITY_SYMBOL
        df_bronze_securities = df_bronze_securities.with_column(
            "SECURITY_SYMBOL",
            upper(trim(col("SECURITY_SYMBOL")))
        )

        # STANDARDIZE ISIN_CODE
        df_bronze_securities = df_bronze_securities.with_column(
            "ISIN_CODE",
            upper(trim(col("ISIN_CODE")))
            #concat(upper(trim(col("ISIN_CODE"))), lit("0")) # just to make standard 12 characters as there are only 11 characters from source - to avoid rejection
        )

        # VALIDATE ISIN LENGTH - Standard ISIN = 11 (12 characters standard)
        #isin_regex = '^[A-Z]{2}[A-Z0-9]{9}[0-9]$' 
        isin_regex = '^[A-Z]{2}[A-Z0-9]{9}$' #as the source data have only 11 characters in all records, considering 11 as the requirement for now
        df_bronze_securities = df_bronze_securities.with_column(
            "ISIN_VALID",
            regexp_like(
                col("ISIN_CODE"),
                lit(isin_regex)
            )
        )
                
        # STANDARDIZE SECURITY_TYPE
        df_bronze_securities = df_bronze_securities.with_column(
            "SECURITY_TYPE",
            upper(trim(col("SECURITY_TYPE")))
        )
        df_bronze_securities = df_bronze_securities.with_column(
            "SECURITY_TYPE",
            when(
                col("SECURITY_TYPE").isin(
                    "STOCK",
                    "BOND",
                    "ETF",
                    "MUTUAL FUND",
                    "OPTION",
                    "FUTURE"
                ),
                col("SECURITY_TYPE")
            ).otherwise(lit("UNKNOWN"))
        )
		
        # STANDARDIZE ASSET_CLASS
        df_bronze_securities = df_bronze_securities.with_column(
            "ASSET_CLASS",
            upper(trim(col("ASSET_CLASS")))
        )
        df_bronze_securities = df_bronze_securities.with_column(
            "ASSET_CLASS",
            when(
                col("ASSET_CLASS").isin(
                    "EQUITY",
                    "FIXED INCOME",
                    "COMMODITY",
                    "FOREX",
                    "DERIVATIVE"
                ),
                col("ASSET_CLASS")
            ).otherwise(lit("UNKNOWN"))
        )

        # STANDARDIZE CURRENCY
        df_bronze_securities = df_bronze_securities.with_column(
            "CURRENCY",
            upper(trim(col("CURRENCY")))
        )

        # VALIDATE CURRENCY
        currency_regex = '^[A-Z]{3}$'
        df_bronze_securities = df_bronze_securities.with_column(
            "CURRENCY_VALID",
            regexp_like(
                col("CURRENCY"),
                lit(currency_regex)
            )
        )

        # HANDLE MATURITY_DATE
        df_bronze_securities = df_bronze_securities.with_column(
            "MATURITY_DATE_VALID",
            when(
                (col("SECURITY_TYPE") == "BOND")
                &
                (col("MATURITY_DATE").is_null()),
                lit(False)
            ).otherwise(lit(True))
        )

        # VALIDATE FACE_VALUE
        df_bronze_securities = df_bronze_securities.with_column(
            "FACE_VALUE_VALID",
            when(
                col("FACE_VALUE").is_null()
                |
                (col("FACE_VALUE") <= 0),
                lit(False)
            ).otherwise(lit(True))
        )

        # HANDLE COUPON_RATE - Null for non-bond instruments
        df_bronze_securities = df_bronze_securities.with_column(
            "COUPON_RATE",
            when(
                (col("SECURITY_TYPE") != "BOND")
                &
                (col("COUPON_RATE").is_null()),
                lit(0)
            ).otherwise(col("COUPON_RATE"))
        )

        # VALIDATE EXCHANGE_CODE
        df_bronze_securities = df_bronze_securities.with_column(
            "EXCHANGE_VALID",
            when(
                col("EXCHANGE_CODE").is_null()
                |
                (trim(col("EXCHANGE_CODE")) == ''),
                lit(False)
            ).otherwise(lit(True))
        )

        # VALIDATE SECURITY_NAME
        df_bronze_securities = df_bronze_securities.with_column(
            "SECURITY_NAME_VALID",
            when(
                col("SECURITY_NAME").is_null()
                |
                (trim(col("SECURITY_NAME")) == ''),
                lit(False)
            ).otherwise(lit(True))
        )
		
        # RECORD HASH
        df_bronze_securities = df_bronze_securities.with_column(
            "RECORD_HASH",
			md5(concat_ws(lit("|"), col("SECURITY_ID"), col("CREATED_AT"), col("UPDATED_AT")))
        )

        # ADD AUDIT_JOB_ID
        df_bronze_securities = df_bronze_securities.with_column(
            "AUDIT_JOB_ID",
            lit(p_job_id)
        )

        # REMOVE DUPLICATES - KEEP LATEST UPDATED_AT
        # If records share the same ISIN_CODE after standardization, dedup keeps only one per ISIN (observed in rejected records table)
        window_spec = Window.partition_by(
            col("ISIN_CODE")
        ).order_by(
            col("UPDATED_AT").desc()
        )
        df_bronze_securities = df_bronze_securities.with_column(
            "RN",
            row_number().over(window_spec)
        )
        df_bronze_securities = df_bronze_securities.filter(
            col("RN") == 1
        )

        # DQ STATUS
        df_bronze_securities = df_bronze_securities.with_column(
            "DQ_STATUS",
            when(
                col("ISIN_VALID")
                &
                col("CURRENCY_VALID")
                &
                col("MATURITY_DATE_VALID")
                &
                col("FACE_VALUE_VALID")
                &
                col("EXCHANGE_VALID")
                &
                col("SECURITY_NAME_VALID"),
                lit("VALID")
            ).otherwise(lit("REJECTED"))
        )

        # DQ ERROR MESSAGE
        df_bronze_securities = df_bronze_securities.with_column(
            "DQ_ERROR_MESSAGE",
            when(
                ~col("ISIN_VALID"),
                lit("INVALID_ISIN_CODE")
            )
            .when(
                ~col("CURRENCY_VALID"),
                lit("INVALID_CURRENCY")
            )
            .when(
                ~col("MATURITY_DATE_VALID"),
                lit("MISSING_MATURITY_DATE_FOR_BOND")
            )
            .when(
                ~col("FACE_VALUE_VALID"),
                lit("INVALID_FACE_VALUE")
            )
            .when(
                ~col("EXCHANGE_VALID"),
                lit("MISSING_EXCHANGE_CODE")
            )
            .when(
                ~col("SECURITY_NAME_VALID"),
                lit("MISSING_SECURITY_NAME")
            )
            .otherwise(lit(None))
        )

        # SPLIT VALID / REJECTED
        valid_df_bronze_securities = df_bronze_securities.filter(
            col("DQ_STATUS") == "VALID"
        )

        rejected_df_bronze_securities = df_bronze_securities.filter(
            col("DQ_STATUS") == "REJECTED"
        )

        # LOAD SILVER
        valid_df_bronze_securities.select(
            col("SECURITY_ID"),
            col("SECURITY_SYMBOL"),
            col("SECURITY_NAME"),
            col("ISIN_CODE"),
            col("SECURITY_TYPE"),
            col("ASSET_CLASS"),
            col("EXCHANGE_CODE"),
            col("CURRENCY"),
            col("FACE_VALUE"),
            col("COUPON_RATE"),
            col("MATURITY_DATE"),
            col("CREATED_AT"),
            col("UPDATED_AT"),
            col("RECORD_HASH"),
            col("AUDIT_JOB_ID"),
            col("DQ_STATUS")
        ).write.save_as_table(
            table_name="SILVER.FINANCE.SECURITIES",
            mode="truncate",
            column_order="name"
        )

        # LOAD REJECTED
        rejected_df_bronze_securities.select(
            col("SECURITY_ID"),
            col("SECURITY_SYMBOL"),
            col("SECURITY_NAME"),
            col("ISIN_CODE"),
            col("SECURITY_TYPE"),
            col("ASSET_CLASS"),
            col("EXCHANGE_CODE"),
            col("CURRENCY"),
            col("FACE_VALUE"),
            col("COUPON_RATE"),
            col("MATURITY_DATE"),
            col("CREATED_AT"),
            col("UPDATED_AT"),
            col("AUDIT_JOB_ID"),
            col("DQ_ERROR_MESSAGE").alias("REJECTION_REASON")
        ).write.save_as_table(
            table_name="SILVER.FINANCE.SECURITIES_REJECTED",
            mode="append",
            column_order="name"
        )

        # COUNTS
        rows_inserted = valid_df_bronze_securities.count()
        rows_rejected = rejected_df_bronze_securities.count()

        # RETURN RESULT
        return {
            "STATUS": "SUCCESS",
            "ROWS_PROCESSED": rows_processed,
            "ROWS_INSERTED": rows_inserted,
            "ROWS_REJECTED": rows_rejected,
            "ERROR_MESSAGE": 'Invalid format, if rows rejected'
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