DECLARE
  MAX_ID_TO_REMOVE NUMBER(10);
  --1674923; -- determine ahead of time to the right id as of Aug 1, 201721500000
  GROUP_SIZE_TO_REMOVE NUMBER(5) := 10000; -- the number of records to remove in one commit
  STARTING_ID          NUMBER(10) ;        --id to start from
BEGIN
  SELECT MIN(ID)
  INTO MAX_ID_TO_REMOVE
  FROM OPMP_OWNER.AUDIT_HISTORY
  WHERE AUDIT_DATE > TRUNC(sysdate)-365;
  SELECT MIN(ID) INTO STARTING_ID FROM OPMP_OWNER.AUDIT_HISTORY;
  EXECUTE immediate 'alter session enable parallel dml';
  EXECUTE immediate 'alter table OPMP_OWNER.AUDIT_HISTORY_archive nologging';
  EXECUTE immediate 'alter table OPMP_OWNER.AUDIT_HISTORY nologging';
  EXECUTE immediate 'alter table OPMP_OWNER.AUDIT_HISTORY_pii_archive nologging';
  EXECUTE immediate 'alter table OPMP_OWNER.AUDIT_HISTORY_PII_DATA nologging';
  INSERT
  INTO OPMP_OWNER.archiver_log
    (
      TIME,
      MESSAGE
    )
    VALUES
    (
      SYSTIMESTAMP,
      'Starting AUDIT LOG Archiving, ids from '
      || STARTING_ID
      || ' TO '
      || MAX_ID_TO_REMOVE
    );
  COMMIT;
  LOOP
    IF (STARTING_ID = MAX_ID_TO_REMOVE) THEN
      EXIT;
    END IF;
    STARTING_ID    := STARTING_ID + GROUP_SIZE_TO_REMOVE;
    IF (STARTING_ID > MAX_ID_TO_REMOVE) THEN
      STARTING_ID  := MAX_ID_TO_REMOVE;
    END IF;
    INSERT
    INTO OPMP_OWNER.archiver_log
      (
        TIME,
        MESSAGE
      )
      VALUES
      (
        systimestamp,
        'Planning to move ids up to '
        || STARTING_ID
      );
    COMMIT;
    INSERT
      /*+ append*/
    INTO OPMP_OWNER.AUDIT_HISTORY_archive
      (
        id,
        USERNAME,
        COMPONENT_TYPE,
        action_type,
        ACCOUNT_NAME,
        DESCRIPTION,
        audit_date,
        ADDITIONAL_INFO,
        EVENT_ID,
        SERVICE_NAME,
        CORRELATION_ID,
        USER_AUTHORIZATION_ID
      )
    SELECT
      /* +parallel */
      id,
      USERNAME,
      COMPONENT_TYPE,
      action_type,
      ACCOUNT_NAME,
      DESCRIPTION,
      audit_date,
      ADDITIONAL_INFO,
      EVENT_ID,
      SERVICE_NAME,
      CORRELATION_ID,
      USER_AUTHORIZATION_ID
    FROM OPMP_OWNER.AUDIT_HISTORY
    WHERE id < STARTING_ID;
    COMMIT;
    INSERT
      /*+ append*/
    INTO OPMP_OWNER.AUDIT_HISTORY_pii_archive
      (
        ID,
        AUDIT_HISTORY_ID,
        PII_DATA
      )
    SELECT
      /* +parallel */
      ID,
      AUDIT_HISTORY_ID,
      PII_DATA
    FROM OPMP_OWNER.AUDIT_HISTORY_PII_DATA
    WHERE id < STARTING_ID;
    COMMIT;
    INSERT
    INTO OPMP_OWNER.archiver_log
      (
        TIME,
        MESSAGE
      )
      VALUES
      (
        systimestamp,
        'Copied over ids up to '
        || STARTING_ID
      );
    DELETE /* +parallel */
    FROM OPMP_OWNER.AUDIT_HISTORY_PII_DATA WHERE id < STARTING_ID;
    COMMIT;
    DELETE /* +parallel */
    FROM OPMP_OWNER.AUDIT_HISTORY WHERE id < STARTING_ID;
    COMMIT;
    INSERT
    INTO OPMP_OWNER.archiver_log
      (
        TIME,
        MESSAGE
      )
      VALUES
      (
        systimestamp,
        'Deleted ids up to '
        || STARTING_ID
      );
    COMMIT;
  END LOOP;
  EXECUTE immediate 'alter table OPMP_OWNER.AUDIT_HISTORY_archive logging';
  EXECUTE immediate 'alter table OPMP_OWNER.AUDIT_HISTORY logging';
  EXECUTE immediate 'alter table OPMP_OWNER.AUDIT_HISTORY_pii_archive logging';
  EXECUTE immediate 'alter table OPMP_OWNER.AUDIT_HISTORY_PII_DATA logging';
EXCEPTION
WHEN OTHERS THEN
  --Rollback the changes and then raise the error again.
  DBMS_OUTPUT.PUT_LINE(SQLCODE || '--' ||SQLERRM);
  ROLLBACK;
END;
/
