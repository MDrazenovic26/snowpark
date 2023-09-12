CREATE OR REPLACE PROCEDURE SNEAKERFAQTORY_MARINA.BRONZE.DIM_CUSTOMER_INC()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
INSERT INTO
    SNEAKERFAQTORY_MARINA.BRONZE.KNA1
SELECT
    COALESCE(REPLACE (source.INDEX, ''.0'', ''''), ''-1''),
    COALESCE(source.KUNNR, ''N/A''),
    source.GENDE,
    source.NAME1,
    source.NAME2,
    source.STRAS,
    source.ORT01,
    source.PSTLZ,
    source.LAND1,
    source.LAND2,
    source.RSCON,
    source.TELF1,
    source.REGIO,
    source.BIRTH,
    source.LATI,
    source.LONG,
    source.LASTMODIFIED
FROM
    (SELECT $1 AS INDEX, 
    $2 AS KUNNR, 
    $3 AS GENDE, 
    $4 AS NAME1, 
    $5 AS NAME2, 
    $6 AS STRAS, 
    $7 AS ORT01, 
    $8 AS PSTLZ, 
    $9 AS LAND1, 
    $10 AS LAND2, 
    $11 AS RSCON, 
    $12 AS TELF1, 
    $13 AS REGIO, 
    $14 AS BIRTH, 
    $15 AS LATI, 
    $16 AS LONG, 
    $17 AS LASTMODIFIED
     FROM @SNEAKERFAQTORY_MARINA.STAGING.MARINA_STAGE  
     WHERE METADATA$FILENAME LIKE ''mssql_source/inc/KNA1%'' AND METADATA$FILENAME NOT IN (SELECT FILE_NAME FROM SNEAKERFAQTORY_MARINA.BRONZE.FILES_HISTORY) AND METADATA$FILE_ROW_NUMBER > 1
    ) source
    LEFT JOIN SNEAKERFAQTORY_MARINA.BRONZE.KNA1 bronze ON COALESCE(REPLACE (source.INDEX, ''.0'', ''''), ''-1'') = bronze.INDEX
    AND COALESCE(source.KUNNR, ''N/A'') = bronze.KUNNR
    AND source.LASTMODIFIED = bronze.LASTMODIFIED
WHERE
    (
        bronze.INDEX IS NULL
        AND bronze.KUNNR IS NULL
    )
    OR (
        bronze.GENDE IS NOT NULL
        AND bronze.GENDE <> source.GENDE
    )
    OR (
        source.NAME1 <> bronze.NAME1
        AND bronze.NAME1 IS NOT NULL
    )
    OR (
        source.NAME2 <> bronze.NAME2
        AND bronze.NAME2 IS NOT NULL
    )
    OR (
        source.STRAS <> bronze.STRAS
        AND bronze.STRAS IS NOT NULL
    )
    OR (
        source.ORT01 <> bronze.ORT01
        AND bronze.ORT01 IS NOT NULL
    )
    OR (
        source.PSTLZ <> bronze.PSTLZ
        AND bronze.PSTLZ IS NOT NULL
    )
    OR (
        source.LAND1 <> bronze.LAND1
        AND bronze.LAND1 IS NOT NULL
    )
    OR (
        source.LAND2 <> bronze.LAND2
        AND bronze.LAND2 IS NOT NULL
    )
    OR (
        source.RSCON <> bronze.RSCON
        AND bronze.RSCON IS NOT NULL
    )
    OR (
        source.TELF1 <> bronze.TELF1
        AND bronze.TELF1 IS NOT NULL
    )
    OR (
        source.REGIO <> bronze.REGIO
        AND bronze.REGIO IS NOT NULL
    )
    OR (
        source.BIRTH <> bronze.BIRTH
        AND bronze.BIRTH IS NOT NULL
    )
    OR (
        source.LATI <> bronze.LATI
        AND bronze.LATI IS NOT NULL
    )
    OR (
        source.LONG <> bronze.LONG
        AND bronze.LONG IS NOT NULL
    );

INSERT INTO SNEAKERFAQTORY_MARINA.BRONZE.FILES_HISTORY (FILE_NAME)
SELECT DISTINCT SPLIT_PART(METADATA$FILENAME, ''/'', -1) AS FILE_NAME
FROM @SNEAKERFAQTORY_MARINA.STAGING.MARINA_STAGE  (file_format => SNEAKERFAQTORY_MARINA.STAGING.CSV_FORMAT)
WHERE METADATA$FILENAME LIKE ''mssql_source/inc/KNA1%''
AND NOT EXISTS (
    SELECT 1
    FROM SNEAKERFAQTORY_MARINA.BRONZE.FILES_HISTORY fh
    WHERE fh.FILE_NAME = SPLIT_PART(METADATA$FILENAME, ''/'', -1)
);

MERGE INTO SNEAKERFAQTORY_MARINA.SILVER.CUSTOMER AS target USING (
    SELECT
        source.*
    FROM
        (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        INDEX,
                        KUNNR
                    ORDER BY
                        LASTMODIFIED DESC
                ) as row_num
            FROM
                SNEAKERFAQTORY_MARINA.BRONZE.KNA1
        ) source
    WHERE
        source.row_num = 1
) AS source ON target.ID = SHA2 (source.INDEX || source.KUNNR, 256) WHEN MATCHED THEN
UPDATE
SET
    target.GENDER = COALESCE(source.GENDE, ''N/A''),
    target.FIRSTNAME = COALESCE(source.NAME1, ''N/A''),
    target.LASTNAME = COALESCE(source.NAME2, ''N/A''),
    target.STREETADDRESS = COALESCE(source.STRAS, ''N/A''),
    target.CITY = COALESCE(source.ORT01, ''N/A''),
    target.POSTALCODE = COALESCE(source.PSTLZ, ''N/A''),
    target.COUNTRYCODE = COALESCE(source.LAND1, ''N/A''),
    target.COUNTRYNAME = COALESCE(source.LAND2, ''N/A''),
    target.EMAILADDRESS = COALESCE(source.RSCON, ''N/A''),
    target.PHONENUMBER = COALESCE(
        REPLACE (REPLACE (source.TELF1, '' '', ''''), ''-'', ''''),
        ''N/A''
    ),
    target.PHONECOUNTRYCODE = COALESCE(source.REGIO, 0),
    target.DATEOFBIRTH = COALESCE(
        TO_CHAR (
            TO_DATE (source.BIRTH, ''YYYY/MM/DD''),
            ''YYYY-MM-DD''
        ),
        ''1900-01-01''
    ),
    target.LATITUDE = COALESCE(CAST(source.LATI AS NUMBER (10, 6)), 0),
    target.LONGITUDE = COALESCE(CAST(source.LONG AS NUMBER (10, 6)), 0),
    target.LASTMODIFIED = source.LASTMODIFIED WHEN NOT MATCHED THEN INSERT (
        ID,
        CUSTOMERID,
        GENDER,
        FIRSTNAME,
        LASTNAME,
        STREETADDRESS,
        CITY,
        POSTALCODE,
        COUNTRYCODE,
        COUNTRYNAME,
        EMAILADDRESS,
        PHONENUMBER,
        PHONECOUNTRYCODE,
        DATEOFBIRTH,
        LATITUDE,
        LONGITUDE,
        LASTMODIFIED
    )
VALUES
    (
        SHA2 (source.INDEX || source.KUNNR, 256),
        source.KUNNR,
        COALESCE(source.GENDE, ''N/A''),
        COALESCE(source.NAME1, ''N/A''),
        COALESCE(source.NAME2, ''N/A''),
        COALESCE(source.STRAS, ''N/A''),
        COALESCE(source.ORT01, ''N/A''),
        COALESCE(source.PSTLZ, ''N/A''),
        COALESCE(source.LAND1, ''N/A''),
        COALESCE(source.LAND2, ''N/A''),
        COALESCE(source.RSCON, ''N/A''),
        COALESCE(
            REPLACE (REPLACE (source.TELF1, '' '', ''''), ''-'', ''''),
            ''N/A''
        ),
        COALESCE(source.REGIO, 0),
        COALESCE(
            TO_CHAR (
                TO_DATE (source.BIRTH, ''YYYY/MM/DD''),
                ''YYYY-MM-DD''
            ),
            ''1900-01-01''
        ),
        COALESCE(CAST(source.LATI AS NUMBER (10, 6)), 0),
        COALESCE(CAST(source.LONG AS NUMBER (10, 6)), 0),
        source.LASTMODIFIED
    );

MERGE INTO SNEAKERFAQTORY_MARINA.GOLD.DIM_CUSTOMER AS target USING (
    SELECT
        ID,
        CUSTOMERID,
        GENDER,
        FIRSTNAME,
        LASTNAME,
        STREETADDRESS,
        CITY,
        POSTALCODE,
        COUNTRYCODE,
        COUNTRYNAME,
        EMAILADDRESS,
        PHONENUMBER,
        PHONECOUNTRYCODE,
        DATEOFBIRTH,
        LATITUDE,
        LONGITUDE,
        LASTMODIFIED
    FROM
        SNEAKERFAQTORY_MARINA.SILVER.CUSTOMER
    WHERE
        ID <> ''-1''
        AND CUSTOMERID <> ''N/A''
) AS source ON target.ID = source.ID WHEN MATCHED THEN
UPDATE
SET
    target.CUSTOMERID = source.CUSTOMERID,
    target.GENDER = source.GENDER,
    target.FIRSTNAME = source.FIRSTNAME,
    target.LASTNAME = source.LASTNAME,
    target.STREETADDRESS = source.STREETADDRESS,
    target.CITY = source.CITY,
    target.POSTALCODE = source.POSTALCODE,
    target.COUNTRYCODE = source.COUNTRYCODE,
    target.COUNTRYNAME = source.COUNTRYNAME,
    target.EMAILADDRESS = source.EMAILADDRESS,
    target.PHONENUMBER = source.PHONENUMBER,
    target.PHONECOUNTRYCODE = source.PHONECOUNTRYCODE,
    target.DATEOFBIRTH = source.DATEOFBIRTH,
    target.LATITUDE = source.LATITUDE,
    target.LONGITUDE = source.LONGITUDE,
    target.LASTMODIFIED = source.LASTMODIFIED WHEN NOT MATCHED THEN INSERT (
        ID,
        CUSTOMERID,
        GENDER,
        FIRSTNAME,
        LASTNAME,
        STREETADDRESS,
        CITY,
        POSTALCODE,
        COUNTRYCODE,
        COUNTRYNAME,
        EMAILADDRESS,
        PHONENUMBER,
        PHONECOUNTRYCODE,
        DATEOFBIRTH,
        LATITUDE,
        LONGITUDE,
        LASTMODIFIED
    )
VALUES
    (
        source.ID,
        source.CUSTOMERID,
        source.GENDER,
        source.FIRSTNAME,
        source.LASTNAME,
        source.STREETADDRESS,
        source.CITY,
        source.POSTALCODE,
        source.COUNTRYCODE,
        source.COUNTRYNAME,
        source.EMAILADDRESS,
        source.PHONENUMBER,
        source.PHONECOUNTRYCODE,
        source.DATEOFBIRTH,
        source.LATITUDE,
        source.LONGITUDE,
        source.LASTMODIFIED
    );

COMMIT;

RETURN ''Successfully executed'';

EXCEPTION WHEN OTHER THEN ROLLBACK;

RAISE;

END;
';