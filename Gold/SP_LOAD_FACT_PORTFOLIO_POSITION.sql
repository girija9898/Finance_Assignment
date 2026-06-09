CREATE OR REPLACE PROCEDURE GOLD.UTILS.SP_LOAD_FACT_PORTFOLIO_POSITION()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE

    V_JOB_ID STRING DEFAULT UUID_STRING();
    V_JOB_NAME STRING DEFAULT 'SP_LOAD_FACT_PORTFOLIO_POSITION';
    V_LAYER_NAME STRING DEFAULT 'GOLD';
    V_STATUS STRING;
    V_START_TIME TIMESTAMP;
    V_END_TIME TIMESTAMP;
    V_ROWS_PROCESSED NUMBER DEFAULT 0;
    V_ROWS_INSERTED NUMBER DEFAULT 0;
    V_ROWS_UPDATED NUMBER DEFAULT 0;
    V_ROWS_FAILED NUMBER DEFAULT 0;
    V_ERROR_MESSAGE STRING;

BEGIN

    -- START TIME
    V_START_TIME := CURRENT_TIMESTAMP();
    V_STATUS := 'STARTED';

    -- AUDIT START
    INSERT INTO GOLD.FINANCE.AUDIT_JOB_LOG
    (
        JOB_ID,
        JOB_NAME,
        LAYER_NAME,
        SOURCE_OBJECT,
        TARGET_OBJECT,
        START_TIME,
        JOB_STATUS
    )
    VALUES
    (
        :V_JOB_ID,
        :V_JOB_NAME,
        :V_LAYER_NAME,
        'SILVER.FINANCE.MARKET_PRICES',
        'GOLD.FINANCE.FACT_PORTFOLIO_POSITION',
        :V_START_TIME,
        :V_STATUS
    );

    -- LATEST MARKET PRICE: latest available adjusted_close_price 
    CREATE OR REPLACE TEMP TABLE GOLD.FINANCE.TMP_LATEST_PRICE AS
    SELECT
        SECURITY_ID,
        ADJUSTED_CLOSE_PRICE,
        FX_RATE_TO_BASE,
        PRICE_CURRENCY,
        PRICE_DATE
    FROM
    (
        SELECT
            SECURITY_ID,
            ADJUSTED_CLOSE_PRICE,
            FX_RATE_TO_BASE,
            PRICE_CURRENCY,
            PRICE_DATE,
            ROW_NUMBER() OVER
            (
                PARTITION BY SECURITY_ID
                ORDER BY PRICE_DATE DESC
            ) AS RN

        FROM SILVER.FINANCE.MARKET_PRICES
    )
    WHERE RN = 1;

    -- PREPARE PORTFOLIO DATA
    CREATE OR REPLACE TEMP TABLE GOLD.FINANCE.TMP_PORTFOLIO AS
    SELECT
        FT.ACCOUNT_SK,
        FT.CUSTOMER_SK,
        FT.SECURITY_SK,
        CURRENT_DATE AS POSITION_DATE,
        -- TOTAL BUY QUANTITY
        SUM(
            CASE
                WHEN UPPER(FT.TRADE_TYPE) = 'BUY'
                THEN FT.QUANTITY
                ELSE 0
            END
        ) AS TOTAL_BUY_QUANTITY,

        -- TOTAL SELL QUANTITY
        SUM(
            CASE
                WHEN UPPER(FT.TRADE_TYPE) = 'SELL'
                THEN FT.QUANTITY
                ELSE 0
            END
        ) AS TOTAL_SELL_QUANTITY,

        -- NET QUANTITY
        SUM(FT.SIGNED_QUANTITY) AS NET_QUANTITY, -- we have added +/- in the Fact_Trade, so it will calculate net_quantity as total_buy_quantity - total_sell_quantity
        
        -- AVERAGE BUY PRICE
        SUM(
            CASE 
                WHEN UPPER(FT.TRADE_TYPE) = 'BUY'
                THEN FT.GROSS_TRADE_AMOUNT
                ELSE 0
            END
        )
        /
        NULLIF
        (
            SUM
            (
                CASE
                    WHEN UPPER(FT.TRADE_TYPE) = 'BUY'
                    THEN FT.QUANTITY
                    ELSE 0
                END
            ),
            0
        )
        AS AVERAGE_BUY_PRICE,

        -- COST BASIS AMOUNT
        (SUM(FT.SIGNED_QUANTITY)) * 
        (SUM
            (
                CASE
                    WHEN UPPER(FT.TRADE_TYPE) = 'BUY'
                    THEN FT.GROSS_TRADE_AMOUNT
                    ELSE 0
                END
            )
            /
            NULLIF
            (SUM
                (
                    CASE
                        WHEN UPPER(FT.TRADE_TYPE) = 'BUY'
                        THEN FT.QUANTITY
                        ELSE 0
                    END
                ),
                0
            )
        )
        AS COST_BASIS_AMOUNT,
        
        -- MARKET PRICE
        COALESCE(MP.ADJUSTED_CLOSE_PRICE, 0) AS MARKET_PRICE,

        -- MARKET VALUE
        (SUM(FT.SIGNED_QUANTITY) * COALESCE(MP.ADJUSTED_CLOSE_PRICE, 0) * COALESCE(MP.FX_RATE_TO_BASE, 1)) AS MARKET_VALUE,

        -- UNREALIZED GAIN LOSS
        (
            (SUM(FT.SIGNED_QUANTITY) * COALESCE(MP.ADJUSTED_CLOSE_PRICE, 0) * COALESCE(MP.FX_RATE_TO_BASE, 1)) -
            ((SUM(FT.SIGNED_QUANTITY))* (
                    SUM
                    (
                        CASE
                            WHEN UPPER(FT.TRADE_TYPE) = 'BUY'
                            THEN FT.GROSS_TRADE_AMOUNT
                            ELSE 0
                        END
                    )
                    /
                    NULLIF
                    (
                        SUM
                        (
                            CASE
                                WHEN UPPER(FT.TRADE_TYPE) = 'BUY'
                                THEN FT.QUANTITY
                                ELSE 0
                            END
                        ),
                        0
                    )
                )
            )
        )
        AS UNREALIZED_GAIN_LOSS,
        
        -- UNREALIZED GAIN LOSS %
        ((((SUM(FT.SIGNED_QUANTITY) * COALESCE(MP.ADJUSTED_CLOSE_PRICE, 0) * COALESCE(MP.FX_RATE_TO_BASE, 1)) -
                    (
                        (SUM(FT.SIGNED_QUANTITY)) *
                        (
                            SUM
                            (
                                CASE
                                    WHEN UPPER(FT.TRADE_TYPE) = 'BUY'
                                    THEN FT.GROSS_TRADE_AMOUNT
                                    ELSE 0
                                END
                            )
                            /
                            NULLIF
                            (
                                SUM
                                (
                                    CASE
                                        WHEN UPPER(FT.TRADE_TYPE) = 'BUY'
                                        THEN FT.QUANTITY
                                        ELSE 0
                                    END
                                ),
                                0
                            )
                        )
                    )
                )
                /
                NULLIF
                (((SUM(FT.SIGNED_QUANTITY)) * (
                            SUM
                            (
                                CASE
                                    WHEN UPPER(FT.TRADE_TYPE) = 'BUY'
                                    THEN FT.GROSS_TRADE_AMOUNT
                                    ELSE 0
                                END
                            )
                            /
                            NULLIF
                            (
                                SUM
                                (
                                    CASE
                                        WHEN UPPER(FT.TRADE_TYPE) = 'BUY'
                                        THEN FT.QUANTITY
                                        ELSE 0
                                    END
                                ),
                                0
                            )
                        )
                    ),
                    0
                )
            ) * 100
        )
        AS UNREALIZED_GAIN_LOSS_PCT,
        MP.PRICE_CURRENCY AS BASE_CURRENCY,
        CURRENT_TIMESTAMP() AS LOAD_TIMESTAMP

    FROM GOLD.FINANCE.FACT_TRADE FT

    LEFT JOIN GOLD.FINANCE.DIM_SECURITY DS
           ON FT.SECURITY_SK = DS.SECURITY_SK
          AND DS.IS_CURRENT = TRUE

    LEFT JOIN GOLD.FINANCE.TMP_LATEST_PRICE MP
           ON DS.SECURITY_ID = MP.SECURITY_ID

    GROUP BY
        FT.ACCOUNT_SK,
        FT.CUSTOMER_SK,
        FT.SECURITY_SK,
        MP.ADJUSTED_CLOSE_PRICE,
        MP.FX_RATE_TO_BASE,
        MP.PRICE_CURRENCY;

    -- PORTFOLIO WEIGHT %
    CREATE OR REPLACE TEMP TABLE GOLD.FINANCE.TMP_FINAL_PORTFOLIO AS
    SELECT
        T.*,
        (T.MARKET_VALUE / NULLIF (SUM(T.MARKET_VALUE) OVER (PARTITION BY T.ACCOUNT_SK, T.POSITION_DATE), 0)) * 100 AS PORTFOLIO_WEIGHT_PCT
    FROM GOLD.FINANCE.TMP_PORTFOLIO T;

    -- ROWS PROCESSED
    SELECT COUNT(*) INTO :V_ROWS_PROCESSED FROM GOLD.FINANCE.TMP_FINAL_PORTFOLIO;

    -- INCREMENTAL LOAD
    INSERT INTO GOLD.FINANCE.FACT_PORTFOLIO_POSITION
    (
        ACCOUNT_SK,
        CUSTOMER_SK,
        SECURITY_SK,
        POSITION_DATE,
        TOTAL_BUY_QUANTITY,
        TOTAL_SELL_QUANTITY,
        NET_QUANTITY,
        AVERAGE_BUY_PRICE,
        COST_BASIS_AMOUNT,
        MARKET_PRICE,
        MARKET_VALUE,
        UNREALIZED_GAIN_LOSS,
        UNREALIZED_GAIN_LOSS_PCT,
        PORTFOLIO_WEIGHT_PCT,
        BASE_CURRENCY,
        LOAD_TIMESTAMP
    )
    SELECT
        P.ACCOUNT_SK,
        P.CUSTOMER_SK,
        P.SECURITY_SK,
        P.POSITION_DATE,
        P.TOTAL_BUY_QUANTITY,
        P.TOTAL_SELL_QUANTITY,
        P.NET_QUANTITY,
        P.AVERAGE_BUY_PRICE,
        P.COST_BASIS_AMOUNT,
        P.MARKET_PRICE,
        P.MARKET_VALUE,
        P.UNREALIZED_GAIN_LOSS,
        P.UNREALIZED_GAIN_LOSS_PCT,
        P.PORTFOLIO_WEIGHT_PCT,
        P.BASE_CURRENCY,
        P.LOAD_TIMESTAMP
    FROM GOLD.FINANCE.TMP_FINAL_PORTFOLIO P
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM GOLD.FINANCE.FACT_PORTFOLIO_POSITION F
        WHERE F.ACCOUNT_SK = P.ACCOUNT_SK
          AND F.SECURITY_SK = P.SECURITY_SK
          AND F.POSITION_DATE = P.POSITION_DATE
    );

    -- ROWS INSERTED
    V_ROWS_INSERTED := SQLROWCOUNT;

    -- END TIME
    V_END_TIME := CURRENT_TIMESTAMP();
    V_STATUS := 'SUCCESS';

    -- AUDIT SUCCESS
    UPDATE GOLD.FINANCE.AUDIT_JOB_LOG
    SET
        END_TIME = :V_END_TIME,
        JOB_STATUS = :V_STATUS
    WHERE JOB_ID = :V_JOB_ID;

    -- SUCCESS EMAIL
    CALL SYSTEM$SEND_EMAIL(
        'finance_email_notification',
        'kgirija@defteam.co',
        'SUCCESS: ' || :V_JOB_NAME,
        'Job Name: ' || :V_JOB_NAME || '\n' ||
        'Job ID: ' || :V_JOB_ID || '\n' ||
        'Layer: ' || :V_LAYER_NAME || '\n' ||
        'Status: ' || :V_STATUS || '\n' ||
        'Rows Processed: ' || :V_ROWS_PROCESSED || '\n' ||
        'Rows Inserted: ' || :V_ROWS_INSERTED || '\n' ||
        'Rows Rejected: ' || :V_ROWS_FAILED || '\n' ||
        'Execution Time: ' || CURRENT_TIMESTAMP()
    );

    RETURN 'SUCCESS';

EXCEPTION

    WHEN OTHER THEN

        V_ERROR_MESSAGE := SQLERRM;
        V_END_TIME := CURRENT_TIMESTAMP();
        V_STATUS := 'FAILED';

        -- AUDIT FAILURE
        UPDATE GOLD.FINANCE.AUDIT_JOB_LOG
        SET
            END_TIME = :V_END_TIME,
            JOB_STATUS = :V_STATUS,
            ERROR_MESSAGE = :V_ERROR_MESSAGE
        WHERE JOB_ID = :V_JOB_ID;

        -- FAILURE EMAIL
        CALL SYSTEM$SEND_EMAIL(
            'finance_email_notification',
            'kgirija@defteam.co',
            'FAILED: ' || :V_JOB_NAME,
            'Job Name: ' || :V_JOB_NAME || '\n' ||
            'Job ID: ' || :V_JOB_ID || '\n' ||
            'Layer: ' || :V_LAYER_NAME || '\n' ||
            'Status: ' || :V_STATUS || '\n' ||
            'Execution Time: ' || CURRENT_TIMESTAMP() || '\n' ||
            'Error Message: ' || :V_ERROR_MESSAGE
        );

        RETURN 'FAILED: ' || :V_ERROR_MESSAGE;

END;
$$;