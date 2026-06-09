CREATE OR REPLACE PROCEDURE SILVER.UTILS.SP_CUSTOMERS_SILVER_TRANSFORMATIONS(
    JOB_ID STRING
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

def run(session, job_id):

    try:
        session.sql("""
            CREATE OR REPLACE TABLE SILVER.UTILS.TEMP_BRONZE_CUSTOMERS AS
            SELECT * FROM BRONZE.FINANCE.BRONZE_CUSTOMERS_STREAM
        """).collect()

        df_bronze_customers = session.table('SILVER.UTILS.TEMP_BRONZE_CUSTOMERS')
        rows_processed = df_bronze_customers.count()

        # EMAIL STANDARDIZATION
        df_bronze_customers = df_bronze_customers.with_column(
            "EMAIL",
            lower(trim(col("EMAIL")))
        )

        email_pattern = '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9.+-]+$'
        df_bronze_customers = df_bronze_customers.with_column(
            "EMAIL_VALID",
            regexp_like(col("EMAIL"), lit(email_pattern))
        )

        # PHONE NUMBER CLEANING
        df_bronze_customers = df_bronze_customers.with_column(
            "PHONE_NUMBER",
            regexp_replace(
                col("PHONE_NUMBER"),
                '[^0-9]',
                ''
            )
        )

        df_bronze_customers = df_bronze_customers.with_column(
            "PHONE_NUMBER",
            substring(col("PHONE_NUMBER"), 1, 10)
        )

        df_bronze_customers = df_bronze_customers.with_column(
            "PHONE_VALID",
            length(col("PHONE_NUMBER")) == 10
        )

        # TAX IDENTIFIER VALIDATION
        tax_regex = '^\\d{3}-\\d{2}-\\d{4}$'

        df_bronze_customers = df_bronze_customers.with_column(
            "TAX_VALID",
            regexp_like(col("TAX_IDENTIFIER"), lit(tax_regex))
        )

        # RISK PROFILE STANDARDIZATION
        df_bronze_customers = df_bronze_customers.with_column(
            "RISK_PROFILE",
            upper(trim(col("RISK_PROFILE")))
        )
        df_bronze_customers = df_bronze_customers.with_column(
            "RISK_PROFILE",
            when(
                col("RISK_PROFILE").isin("LOW", "MEDIUM", "HIGH"),
                col("RISK_PROFILE")
            ).otherwise(lit("UNKNOWN"))
        )

        # HANDLING NULL NAMES FIRST_NAME, LAST_NAME, ORGANIZATION_NAME
        df_bronze_customers = df_bronze_customers.with_column(
            "FIRST_NAME",
            when(
                (col("CUSTOMER_TYPE") == "INDIVIDUAL") & (col("FIRST_NAME").is_null()),
                lit("UNKNOWN")
            ).otherwise(col("FIRST_NAME"))
        )
        df_bronze_customers = df_bronze_customers.with_column(
            "LAST_NAME",
            when(
                (col("CUSTOMER_TYPE") == "INDIVIDUAL") & (col("LAST_NAME").is_null()),
                lit("UNKNOWN")
            ).otherwise(col("LAST_NAME"))
        )

        df_bronze_customers = df_bronze_customers.with_column(
            "ORGANIZATION_NAME",
            when(
                (col("CUSTOMER_TYPE") == "ORGANIZATION") & (col("ORGANIZATION_NAME").is_null()),
                lit("UNKNOWN_ORG")
            ).otherwise(col("ORGANIZATION_NAME"))
        )

        # RECORD HASH
        df_bronze_customers = df_bronze_customers.with_column(
            "RECORD_HASH_SILVER",
            md5(concat_ws(lit("|"), col("CUSTOMER_ID"), col("CREATED_AT"), col("UPDATED_AT")))
        )

        # DEDUPLICATION
        window_spec = Window.partition_by(
            coalesce(col("TAX_IDENTIFIER"), col("EMAIL"))
        ).order_by(col("UPDATED_AT").desc())

        df_bronze_customers = df_bronze_customers.with_column(
            "RN",
            row_number().over(window_spec)
        )

        df_bronze_customers = df_bronze_customers.filter(col("RN") == 1)

        # DQ STATUS
        df_bronze_customers = df_bronze_customers.with_column(
            "DQ_STATUS",
            when(
                col("EMAIL_VALID") & col("PHONE_VALID") & col("TAX_VALID"),
                lit("VALID")
            ).otherwise(lit("REJECTED"))
        )

        # DQ ERROR MESSAGE
        df_bronze_customers = df_bronze_customers.with_column(
            "DQ_ERROR_MESSAGE",
            when(~col("EMAIL_VALID"), lit("INVALID_EMAIL"))
            .when(~col("PHONE_VALID"), lit("INVALID_PHONE"))
            .when(~col("TAX_VALID"), lit("INVALID_TAX_ID"))
            .otherwise(lit(None))
        )

        # SPLIT VALID / REJECTED
        valid_cust_df = df_bronze_customers.filter(col("DQ_STATUS") == "VALID")
        valid_cust_df = valid_cust_df.with_column("AUDIT_JOB_ID", lit(job_id))

        rejected_cust_df = df_bronze_customers.filter(col("DQ_STATUS") == "REJECTED")

        # LOAD REJECTED
        rejected_cust_df.select(
            col("CUSTOMER_ID"),
            col("EMAIL"),
            col("PHONE_NUMBER"),
            col("TAX_IDENTIFIER"),
            col("DQ_ERROR_MESSAGE").alias("REJECTION_REASON")
        ).write.save_as_table(
            table_name="SILVER.FINANCE.CUSTOMERS_REJECTED",
            mode="append",
            column_order="name"
        )

        # LOAD SILVER
        valid_cust_df.select(
            col("CUSTOMER_ID"),
            col("CUSTOMER_TYPE"),
            col("FIRST_NAME"),
            col("LAST_NAME"),
            col("ORGANIZATION_NAME"),
            col("EMAIL"),
            col("PHONE_NUMBER"),
            col("TAX_IDENTIFIER"),
            col("RISK_PROFILE"),
            col("KYC_STATUS"),
            col("CREATED_AT"),
            col("UPDATED_AT"),
            col("RECORD_HASH_SILVER").alias("RECORD_HASH"),
            col("AUDIT_JOB_ID"),
            col("DQ_STATUS")
        ).write.save_as_table(
            table_name="SILVER.FINANCE.CUSTOMERS",
            mode="truncate",
            column_order="name"
        )

        rows_inserted = valid_cust_df.count()
        rows_rejected = rejected_cust_df.count()

        session.sql("DROP TABLE IF EXISTS SILVER.UTILS.TEMP_BRONZE_CUSTOMERS").collect()

        return {
            "STATUS": "SUCCESS",
            "ROWS_PROCESSED": rows_processed,
            "ROWS_INSERTED": rows_inserted,
            "ROWS_REJECTED": rows_rejected,
            "ERROR_MESSAGE": 'REJECTED REASON INVALID FORMAT, IF ROWS REJECTED'
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
