CREATE OR REPLACE PROCEDURE BRONZE.UTILS.SP_GENERIC_BRONZE_LOAD(
    p_table_name STRING
    -- p_force_reload BOOLEAN DEFAULT FALSE
)
RETURNS STRING
LANGUAGE SQL
AS
$$

DECLARE

    v_job_id STRING;
    v_batch_id STRING;

    v_stage_folder STRING;
    v_target_table STRING;
    v_column_list STRING;
    v_select_list STRING;
    v_job_name STRING;
    v_email STRING;
    v_business_key STRING;

    v_temp_table STRING;

    v_copy_sql STRING;
    v_merge_sql STRING;

    v_cols STRING;
    v_set_clause STRING;
    v_vals_clause STRING;

    v_email_subject STRING;
    v_email_body STRING;

    v_rows_processed NUMBER := 0;
    v_rows_loaded NUMBER := 0;
    v_rows_failed NUMBER := 0;

    v_rows_inserted NUMBER := 0;
    v_rows_updated NUMBER := 0;

    v_error STRING := '';

    v_last_load_time TIMESTAMP_NTZ;

    v_file_list STRING;
    v_has_files NUMBER := 0;

    v_stage_relative_path STRING;

BEGIN

        -- JOB INFO
    v_job_id := 'JOB_' || UUID_STRING();
    v_batch_id := 'BATCH_' || TO_VARCHAR(CURRENT_TIMESTAMP(),'YYYYMMDDHH24MISS');

    -- READ CONFIG
    SELECT
        stage_folder,
        target_table,
        column_list,
        select_list,
        job_name,
        email_recipient,
        business_key
    INTO
        :v_stage_folder,
        :v_target_table,
        :v_column_list,
        :v_select_list,
        :v_job_name,
        :v_email,
        :v_business_key
    FROM BRONZE.UTILS.ETL_CONFIG
    WHERE table_name = :p_table_name;

    -- REPLACE DYNAMIC VALUES
    v_select_list := REPLACE(REPLACE(v_select_list,'DYNAMIC_BATCH_ID',v_batch_id),'DYNAMIC_JOB_ID',v_job_id);

    -- INSERT AUDIT START
    INSERT INTO BRONZE.FINANCE.AUDIT_JOB_LOG
    (
        JOB_ID,
        JOB_NAME,
        LAYER_NAME,
        SOURCE_OBJECT,
        TARGET_OBJECT,
        START_TIME,
        JOB_STATUS,
        LOAD_BATCH_ID
    )
    VALUES
    (
        :v_job_id,
        :v_job_name,
        'BRONZE',
        :v_stage_folder,
        :v_target_table,
        CURRENT_TIMESTAMP(),
        'RUNNING',
        :v_batch_id
    );

    -- GET LAST SUCCESSFUL LOAD TIME
    SELECT COALESCE(MAX(END_TIME), '1900-01-01'::TIMESTAMP_NTZ)
    INTO :v_last_load_time
    FROM BRONZE.FINANCE.AUDIT_JOB_LOG
    WHERE TARGET_OBJECT = :v_target_table
    AND JOB_STATUS = 'SUCCESS';

    -- GET RELATIVE PATH
    v_stage_relative_path := REPLACE(v_stage_folder,'@BRONZE.FINANCE.BRONZE_STAGE/','');

    -- GET FILES TO PROCESS
    SELECT
        LISTAGG('''' ||SPLIT_PART(RELATIVE_PATH, '/', -1)|| '''',','),
        COUNT(*)
    INTO
        :v_file_list,
        :v_has_files
    FROM DIRECTORY(@BRONZE.FINANCE.BRONZE_STAGE)
    WHERE RELATIVE_PATH LIKE :v_stage_relative_path || '%'
    AND
    (   -- p_force_reload = TRUE OR 
		LAST_MODIFIED > :v_last_load_time -- can change as per our requirement to reload the same file like '2026-05-27 06:30:21.818'
    )
    AND SPLIT_PART(RELATIVE_PATH, '/', -1) NOT IN (
        SELECT DISTINCT SOURCE_FILE_NAME
        FROM IDENTIFIER(:v_target_table)
    );

    -- EXIT IF NO FILES
    IF (v_has_files = 0) THEN
    
        UPDATE BRONZE.FINANCE.AUDIT_JOB_LOG
        SET
            END_TIME = CURRENT_TIMESTAMP(),
            JOB_STATUS = 'SUCCESS',
            ERROR_MESSAGE = 'NO NEW FILES FOUND'
        WHERE JOB_ID = :v_job_id;

        RETURN 'NO NEW FILES TO PROCESS';

    END IF;

    -- CREATE TEMP TABLE
    v_temp_table := v_target_table || '_STG_TEMP';
    EXECUTE IMMEDIATE
        'CREATE OR REPLACE TEMPORARY TABLE ' || v_temp_table || ' LIKE ' || v_target_table;

    -- COPY INTO TEMP TABLE
    v_copy_sql := '
        COPY INTO ' || v_temp_table || ' (' || v_column_list || ')
        FROM
        (
            SELECT ' || v_select_list || ' FROM ' || v_stage_folder || ' t
        )
        FILES = (' || v_file_list || ')
        FILE_FORMAT = BRONZE.UTILS.CSV_FILE_FORMAT
        ON_ERROR = ''CONTINUE''
    ';

    EXECUTE IMMEDIATE :v_copy_sql;

    -- COPY RESULT
    SELECT
        COALESCE(SUM("rows_loaded"),0),
        COALESCE(SUM("errors_seen"),0),
        LISTAGG(DISTINCT "first_error", '; ')
    INTO
        :v_rows_loaded,
        :v_rows_failed,
        :v_error
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    SELECT COUNT(*) INTO :v_rows_processed
    FROM IDENTIFIER(:v_temp_table);

    -- PREPARE MERGE
    SELECT REGEXP_REPLACE(REPLACE(:v_column_list, CHR(10), ''),'\\s+','') INTO :v_cols;
    SELECT REGEXP_REPLACE(:v_cols,'([^,]+)','tgt.\\1 = src.\\1') INTO :v_set_clause;
    SELECT REGEXP_REPLACE(:v_cols,'([^,]+)','src.\\1') INTO :v_vals_clause;

    -- MERGE (using distinct records from temp to avoid duplicates)
    v_merge_sql := '
        MERGE INTO ' || v_target_table || ' tgt
        USING (
            SELECT * FROM ' || v_temp_table || '
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ' || v_business_key || ' ORDER BY ' || v_business_key || ') = 1
        ) src
        ON tgt.' || v_business_key || ' = src.' || v_business_key || '

        WHEN MATCHED AND tgt.RECORD_HASH != src.RECORD_HASH
        THEN UPDATE SET ' || v_set_clause || '

        WHEN NOT MATCHED 
        THEN INSERT (' || v_cols || ')
        VALUES (' || v_vals_clause || ')';

    EXECUTE IMMEDIATE :v_merge_sql;

    -- DROP TEMP TABLE
    EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS ' || v_temp_table;

    -- UPDATE AUDIT SUCCESS
    UPDATE BRONZE.FINANCE.AUDIT_JOB_LOG
    SET
        END_TIME = CURRENT_TIMESTAMP(),
        JOB_STATUS = 'SUCCESS',
        ROWS_PROCESSED = :v_rows_processed,
        ROWS_INSERTED = :v_rows_loaded,
        ROWS_FAILED = :v_rows_failed
    WHERE JOB_ID = :v_job_id;

    -- CONSUMING THE STREAM, JUST TO EMPTY IT
    CREATE OR REPLACE TEMP TABLE TMP_STREAM_CONSUME AS
    SELECT * FROM BRONZE.FINANCE.STR_BRONZE_STAGE;
    -- DROP THE TEMP TABLE
    DROP TABLE TMP_STREAM_CONSUME;

    -- EMAIL SUCCESS
    v_email_subject := 'SUCCESS : ' || v_job_name;

    v_email_body := 'Batch ID    : ' || v_batch_id || '\n' ||
                    'Job Name: ' || :v_job_name || '\n' ||
                    'Job ID: ' || :v_job_id || '\n' ||
                    'Layer: ' || 'BRONZE' || '\n' ||
                    'Status: ' || 'SUCCESS' || '\n' ||
                    'Rows Loaded: ' || TO_VARCHAR(v_rows_loaded) || '\n' ||
                    'Rows Failed: ' || TO_VARCHAR(v_rows_failed) || '\n' ||
                    'Failed Reason : ' || NVL(v_error, 'N/A') || '\n' ||
                    'Execution Time: ' || CURRENT_TIMESTAMP();
                    
    CALL SYSTEM$SEND_EMAIL(
        'finance_email_notification', 
        :v_email, 
        :v_email_subject, 
        :v_email_body
    );
    RETURN 'LOAD SUCCESS';

EXCEPTION

    WHEN OTHER THEN
        LET v_err_msg STRING := SQLERRM;
        UPDATE BRONZE.FINANCE.AUDIT_JOB_LOG
        SET
            END_TIME = CURRENT_TIMESTAMP(),
            JOB_STATUS = 'FAILED',
            ERROR_MESSAGE = :v_err_msg
        WHERE JOB_ID = :v_job_id;

        v_email_subject := 'FAILED : ' || v_job_name;

        v_email_body := 'Batch ID    : ' || v_batch_id || '\n' ||
                        'Job Name: ' || :v_job_name || '\n' ||
                        'Job ID: ' || :v_job_id || '\n' ||
                        'Layer: ' || 'BRONZE' || '\n' ||
                        'Status: ' || 'FAILED' || '\n' ||
                        -- 'Rows Loaded: ' || TO_VARCHAR(v_rows_loaded) || '\n' ||
                        -- 'Rows Failed: ' || TO_VARCHAR(v_rows_failed) || '\n' ||
                        'Failed Reason : ' || v_err_msg || '\n' ||
                        'Execution Time: ' || CURRENT_TIMESTAMP();
        
        CALL SYSTEM$SEND_EMAIL(
            'finance_email_notification', 
            :v_email, 
            :v_email_subject, 
            :v_email_body
        );
        RETURN 'LOAD FAILED';
END;

$$;