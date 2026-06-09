CREATE OR REPLACE PROCEDURE SILVER.UTILS.SP_GENERIC_SILVER_LOAD
(
    P_JOB_NAME STRING,
    P_LAYER_NAME STRING,
    P_SOURCE_OBJECT STRING,
    P_TARGET_OBJECT STRING,
    P_TRANSFORMATION_SP STRING,
    P_EMAIL_ADDRESS STRING
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$

DECLARE

    V_JOB_ID STRING;
    V_SQL STRING;
    V_RESULT VARIANT;
    V_STATUS STRING;
    V_ROWS_PROCESSED NUMBER;
    V_ROWS_INSERTED NUMBER;
    V_ROWS_REJECTED NUMBER;
    V_ERROR_MESSAGE STRING;

BEGIN

    -- GENERATE JOB ID
    V_JOB_ID := 'JOB_' || UUID_STRING();

    -- AUDIT START 
    INSERT INTO SILVER.FINANCE.AUDIT_JOB_LOG
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
        :P_JOB_NAME,
        :P_LAYER_NAME,
        :P_SOURCE_OBJECT,
        :P_TARGET_OBJECT,
        CURRENT_TIMESTAMP(),
        'STARTED'
    );
    
        
    -- DYNAMIC PROCEDURE CALL
    V_SQL :=
        'CALL ' || P_TRANSFORMATION_SP || '(''' || V_JOB_ID || ''')';

    EXECUTE IMMEDIATE :V_SQL;										 
				
    -- GET RESULT
    V_RESULT := (
        SELECT PARSE_JSON($1)
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    );

    -- PARSE RESULT
    V_STATUS :=
        V_RESULT:"STATUS"::STRING;

    V_ROWS_PROCESSED :=
        V_RESULT:"ROWS_PROCESSED"::NUMBER;

    V_ROWS_INSERTED :=
        V_RESULT:"ROWS_INSERTED"::NUMBER;

    V_ROWS_REJECTED :=
        V_RESULT:"ROWS_REJECTED"::NUMBER;

    V_ERROR_MESSAGE :=
        V_RESULT:"ERROR_MESSAGE"::STRING;

    -- UPDATE AUDIT
    UPDATE SILVER.FINANCE.AUDIT_JOB_LOG
    SET
        END_TIME = CURRENT_TIMESTAMP(),
        JOB_STATUS = :V_STATUS,
        ROWS_PROCESSED = :V_ROWS_PROCESSED,
        ROWS_INSERTED = :V_ROWS_INSERTED,
        ROWS_FAILED = :V_ROWS_REJECTED,
        ERROR_MESSAGE = :V_ERROR_MESSAGE
    WHERE JOB_ID = :V_JOB_ID;

    -- SUCCESS EMAIL
    CALL SYSTEM$SEND_EMAIL(
        'finance_email_notification',
        :P_EMAIL_ADDRESS,
        'SUCCESS: ' || :P_JOB_NAME,
        'Job Name: ' || :P_JOB_NAME || '\n' ||
        'Job ID: ' || :V_JOB_ID || '\n' ||
        'Layer: ' || :P_LAYER_NAME || '\n' ||
        'Status: ' || :V_STATUS || '\n' ||
        'Rows Processed: ' || :V_ROWS_PROCESSED || '\n' ||
        'Rows Inserted: ' || :V_ROWS_INSERTED || '\n' ||
        'Rows Rejected: ' || :V_ROWS_REJECTED || '\n' ||
        'Execution Time: ' || CURRENT_TIMESTAMP()
    );

    RETURN 'SUCCESS';

EXCEPTION

    WHEN OTHER THEN

        V_ERROR_MESSAGE := SQLERRM;

        -- UPDATE AUDIT FAILURE
        UPDATE SILVER.FINANCE.AUDIT_JOB_LOG
        SET
            END_TIME = CURRENT_TIMESTAMP(),
            JOB_STATUS = 'FAILED',
            ERROR_MESSAGE = :V_ERROR_MESSAGE
        WHERE JOB_ID = :V_JOB_ID;

        -- FAILURE EMAIL
        CALL SYSTEM$SEND_EMAIL(
            'finance_email_notification',
            :P_EMAIL_ADDRESS,
            'FAILED: ' || :P_JOB_NAME,
            'Job Name: ' || :P_JOB_NAME || '\n' ||
            'Job ID: ' || :V_JOB_ID || '\n' ||
            'Layer: ' || :P_LAYER_NAME || '\n' ||
            'Status: ' || :V_STATUS || '\n' ||
            'Rows Processed: ' || :V_ROWS_PROCESSED || '\n' ||
            'Rows Inserted: ' || :V_ROWS_INSERTED || '\n' ||
            'Rows Rejected: ' || :V_ROWS_REJECTED || '\n' ||
            'Execution Time: ' || CURRENT_TIMESTAMP() || '\n' ||
            'Error Message: ' || :V_ERROR_MESSAGE
        );

        RETURN 'FAILED';

END;   

$$;
