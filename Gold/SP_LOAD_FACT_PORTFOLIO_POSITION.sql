-- Procedure to load FACT_PORTFOLIO_POSITION from GOLD.FINANCE.FACT_MARKET_PRICE and FACT_TRADE

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
        'GOLD.FINANCE.FACT_MARKET_PRICE',
        'GOLD.FINANCE.FACT_PORTFOLIO_POSITION',
        :V_START_TIME,
        :V_STATUS
    );

    -- LATEST MARKET PRICE: latest available adjusted_close_price 
    CREATE OR REPLACE TEMP TABLE GOLD.FINANCE.TMP_LATEST_PRICE AS
    SELECT
        SECURITY_SK,
        ADJUSTED_CLOSE_PRICE,
        FX_RATE_TO_BASE,
        PRICE_CURRENCY,
        PRICE_DATE
    FROM
    (
        SELECT
            SECURITY_SK,
            ADJUSTED_CLOSE_PRICE,
            FX_RATE_TO_BASE,
            PRICE_CURRENCY,
            PRICE_DATE,
            ROW_NUMBER() OVER
            (
                PARTITION BY SECURITY_SK
                ORDER BY PRICE_DATE DESC
            ) AS RN

        FROM GOLD.FINANCE.FACT_MARKET_PRICE
    )
    WHERE RN = 1;

    -- PREPARE PORTFOLIO DATA
    
    CREATE OR REPLACE TEMP TABLE GOLD.FINANCE.TMP_PORTFOLIO AS
    --  CTE START
    WITH TRADE_AGG AS
    (
        SELECT
            FT.ACCOUNT_SK,
            FT.CUSTOMER_SK,
            FT.SECURITY_SK,
    
            SUM(
                CASE
                    WHEN UPPER(FT.TRADE_TYPE) = 'BUY'
                    THEN FT.QUANTITY
                    ELSE 0
                END
            ) AS TOTAL_BUY_QUANTITY,
    
            SUM(
                CASE
                    WHEN UPPER(FT.TRADE_TYPE) = 'SELL'
                    THEN FT.QUANTITY
                    ELSE 0
                END
            ) AS TOTAL_SELL_QUANTITY,
    
            SUM(FT.SIGNED_QUANTITY) AS NET_QUANTITY,
    
            SUM(
                CASE
                    WHEN UPPER(FT.TRADE_TYPE) = 'BUY'
                    THEN FT.GROSS_TRADE_AMOUNT
                    ELSE 0
                END
            ) AS TOTAL_BUY_AMOUNT 
    
        FROM GOLD.FINANCE.FACT_TRADE FT
        GROUP BY
            FT.ACCOUNT_SK,
            FT.CUSTOMER_SK,
            FT.SECURITY_SK
    )
    --  CTE END

    SELECT
        A.ACCOUNT_SK,
        A.CUSTOMER_SK,
        A.SECURITY_SK,
    
        TMP.PRICE_DATE AS POSITION_DATE,
    
        A.TOTAL_BUY_QUANTITY,
        A.TOTAL_SELL_QUANTITY,
        A.NET_QUANTITY,
    
        -- AVERAGE BUY PRICE
        A.TOTAL_BUY_AMOUNT
            / NULLIF(A.TOTAL_BUY_QUANTITY,0)
            AS AVERAGE_BUY_PRICE,
    
        -- COST BASIS AMOUNT
        A.NET_QUANTITY * AVERAGE_BUY_PRICE AS COST_BASIS_AMOUNT,
        --(
        --    A.TOTAL_BUY_AMOUNT
        --    / NULLIF(A.TOTAL_BUY_QUANTITY,0)
        --) AS COST_BASIS_AMOUNT,
    
        -- MARKET PRICE
        COALESCE(TMP.ADJUSTED_CLOSE_PRICE,0) AS MARKET_PRICE,
    
        -- MARKET VALUE
        (
            A.NET_QUANTITY
            * COALESCE(TMP.ADJUSTED_CLOSE_PRICE,0)
            * COALESCE(TMP.FX_RATE_TO_BASE,1)
        ) AS MARKET_VALUE,
    
        -- UNREALIZED GAIN LOSS
        ( MARKET_VALUE - COST_BASIS_AMOUNT
            -- (
            --     A.NET_QUANTITY
            --     * COALESCE(TMP.ADJUSTED_CLOSE_PRICE,0)
            --     * COALESCE(TMP.FX_RATE_TO_BASE,1)
            -- )
            -- -
            -- (
            --    A.NET_QUANTITY
            --     *
            --     (
            --         A.TOTAL_BUY_AMOUNT
            --         / NULLIF(A.TOTAL_BUY_QUANTITY,0)
            --     )
            -- )
        ) AS UNREALIZED_GAIN_LOSS,
    
        -- UNREALIZED GAIN LOSS %
        ( UNREALIZED_GAIN_LOSS / NULLIF(COST_BASIS_AMOUNT, 0) 
            -- (
            --     (
            --         A.NET_QUANTITY
            --         * COALESCE(TMP.ADJUSTED_CLOSE_PRICE,0)
            --        * COALESCE(TMP.FX_RATE_TO_BASE,1)
            --     )
            --     -
            --     (
            --         A.NET_QUANTITY
            --        *
            --        (
            --             A.TOTAL_BUY_AMOUNT
            --             / NULLIF(A.TOTAL_BUY_QUANTITY,0)
            --         )
            --     )
            -- )
            -- /
            -- NULLIF
            -- (
            --     (
            --         A.NET_QUANTITY
            --         *
            --         (
            --             A.TOTAL_BUY_AMOUNT
            --             / NULLIF(A.TOTAL_BUY_QUANTITY,0)
            --         )
            --    ),
            --     0
            -- )
        ) * 100 AS UNREALIZED_GAIN_LOSS_PCT,
    
        TMP.PRICE_CURRENCY AS BASE_CURRENCY,
        CURRENT_TIMESTAMP() AS LOAD_TIMESTAMP
    
    FROM TRADE_AGG A
    
    LEFT JOIN GOLD.FINANCE.TMP_LATEST_PRICE TMP
           ON A.SECURITY_SK = TMP.SECURITY_SK;

    -- PORTFOLIO WEIGHT %
    CREATE OR REPLACE TEMP TABLE GOLD.FINANCE.TMP_FINAL_PORTFOLIO AS
    SELECT
        T.*,
        (T.MARKET_VALUE / NULLIF (SUM(T.MARKET_VALUE) OVER (PARTITION BY T.ACCOUNT_SK, T.POSITION_DATE), 0)) * 100 AS PORTFOLIO_WEIGHT_PCT
    FROM GOLD.FINANCE.TMP_PORTFOLIO T;

    -- ROWS PROCESSED
    SELECT COUNT(*) INTO :V_ROWS_PROCESSED FROM GOLD.FINANCE.TMP_FINAL_PORTFOLIO;

    -- INCREMENTAL LOAD
    MERGE INTO GOLD.FINANCE.FACT_PORTFOLIO_POSITION AS Tgt
    USING GOLD.FINANCE.TMP_FINAL_PORTFOLIO AS Src
    ON  Tgt.ACCOUNT_SK = Src.ACCOUNT_SK
        AND Tgt.SECURITY_SK = Src.SECURITY_SK
        AND Tgt.POSITION_DATE = Src.POSITION_DATE
        
    WHEN MATCHED
	AND (
	-- IS DISTINCT FROM is null-safe operator in Snowflake
		   Tgt.MARKET_PRICE IS DISTINCT FROM Src.MARKET_PRICE
		OR Tgt.MARKET_VALUE IS DISTINCT FROM Src.MARKET_VALUE
	)
	THEN UPDATE
        SET 
    		Tgt.MARKET_PRICE = Src.MARKET_PRICE,
    		Tgt.MARKET_VALUE = Src.MARKET_VALUE,
            Tgt.UNREALIZED_GAIN_LOSS = Src.UNREALIZED_GAIN_LOSS,
            Tgt.UNREALIZED_GAIN_LOSS_PCT = Src.UNREALIZED_GAIN_LOSS_PCT,
            Tgt.PORTFOLIO_WEIGHT_PCT = Src.PORTFOLIO_WEIGHT_PCT,
            Tgt.LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
    		
    WHEN NOT MATCHED THEN
    	INSERT 
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
    	VALUES
    		(
    			Src.ACCOUNT_SK,
    			Src.CUSTOMER_SK,
    			Src.SECURITY_SK,
    			Src.POSITION_DATE,
    			Src.TOTAL_BUY_QUANTITY,
    			Src.TOTAL_SELL_QUANTITY,
    			Src.NET_QUANTITY,
    			Src.AVERAGE_BUY_PRICE,
    			Src.COST_BASIS_AMOUNT,
    			Src.MARKET_PRICE,
    			Src.MARKET_VALUE,
    			Src.UNREALIZED_GAIN_LOSS,
    			Src.UNREALIZED_GAIN_LOSS_PCT,
    			Src.PORTFOLIO_WEIGHT_PCT,
    			Src.BASE_CURRENCY,
    			Src.LOAD_TIMESTAMP
    		);
    
    -- ROWS INSERTED
    V_ROWS_INSERTED := SQLROWCOUNT;

    -- END TIME
    V_END_TIME := CURRENT_TIMESTAMP();
    V_STATUS := 'SUCCESS';

    -- AUDIT SUCCESS
    UPDATE GOLD.FINANCE.AUDIT_JOB_LOG
    SET 
        ROWS_PROCESSED = :V_ROWS_PROCESSED,
        ROWS_INSERTED = :V_ROWS_INSERTED,
        ROWS_FAILED = :V_ROWS_FAILED,
        END_TIME = :V_END_TIME,
        JOB_STATUS = :V_STATUS
    WHERE JOB_ID = :V_JOB_ID;

    -- SUCCESS EMAIL
    CALL SYSTEM$SEND_EMAIL(
        'finance_email_notification',
        'kgirija@defteam.co',
        'SUCCESS : ' || :V_JOB_NAME,
        'Job Name : ' || :V_JOB_NAME || '\n' ||
        'Job ID : ' || :V_JOB_ID || '\n' ||
        'Layer : ' || :V_LAYER_NAME || '\n' ||
        'Status : ' || :V_STATUS || '\n' ||
        'Rows Processed : ' || :V_ROWS_PROCESSED || '\n' ||
        'Rows Inserted : ' || :V_ROWS_INSERTED || '\n' ||
        'Rows Rejected : ' || :V_ROWS_FAILED || '\n' ||
        'Execution Time : ' || CURRENT_TIMESTAMP()
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
            'FAILED : ' || :V_JOB_NAME,
            'Job Name : ' || :V_JOB_NAME || '\n' ||
            'Job ID : ' || :V_JOB_ID || '\n' ||
            'Layer : ' || :V_LAYER_NAME || '\n' ||
            'Status : ' || :V_STATUS || '\n' ||
            'Execution Time : ' || CURRENT_TIMESTAMP() || '\n' ||
            'Error Message : ' || :V_ERROR_MESSAGE
        );

        RETURN 'FAILED: ' || :V_ERROR_MESSAGE;

END;
$$;




