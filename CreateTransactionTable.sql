-- Active: 1687468123942@@127.0.0.1@3306@Groceries
CREATE DEFINER=`root`@`%` PROCEDURE `CreateTransactionTable`()
BEGIN

    DECLARE _Item VARCHAR(50);
    DECLARE _MemberNumber INT;
    DECLARE _Date DATE;

    DECLARE _PrecMember INT DEFAULT NULL;
    DECLARE _PrecDate DATE DEFAULT NULL;
    DECLARE _PrecItem VARCHAR(255);

    DECLARE _TransactionID INT DEFAULT 0;

    DECLARE _Finito BOOL DEFAULT FALSE;

    DECLARE _Cursor CURSOR FOR
    SELECT * FROM Dataset D ORDER BY D.MemberNumber, D._Date;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET _Finito = TRUE;

    SET group_concat_max_len = 1000000;

# CREAZIONE DELLA TABELLA DELLE TRANSAZIONI
#_________________________________________________________________________________________
    DROP TABLE IF EXISTS T ;

    SET @TransactionTable = 'CREATE TABLE T (ID INT PRIMARY KEY';

    SELECT GROUP_CONCAT( CONCAT(ItemDescription, ' BOOLEAN DEFAULT FALSE') SEPARATOR ' ')
    FROM (
        SELECT DISTINCT CONCAT(',`', ItemDescription, '`') AS ItemDescription
        FROM Dataset
    ) AS D
    INTO @tmp;

    SET @TransactionTable = CONCAT(@TransactionTable, @tmp, ')');

    PREPARE stmt FROM @TransactionTable;
    EXECUTE stmt;
#_________________________________________________________________________________________

    OPEN _Cursor;
        _Fetch: LOOP
            FETCH _Cursor INTO _MemberNumber, _Date, _Item;

            IF _Finito IS TRUE THEN
                LEAVE _Fetch;
            END IF;

            IF (_PrecMember IS NULL AND _PrecDate IS NULL) THEN
                SET _PrecDate = _Date;
                SET _PrecMember = _MemberNumber;
                SET _PrecItem = _Item;

                SET @query = CONCAT('INSERT INTO T(ID) VALUES(',_TransactionID,')');

                PREPARE stmt FROM @query;
                EXECUTE stmt;
            END IF;

            IF (_MemberNumber <> _PrecMember OR _Date <> _PrecDate) THEN
                SET _PrecDate = _Date;
                SET _PrecMember = _MemberNumber;
                SET _PrecItem = _Item;
                SET _TransactionID = _TransactionID + 1;

                SET @query = CONCAT('INSERT INTO T(ID) VALUES(',_TransactionID,')');
                PREPARE stmt FROM @query;
                EXECUTE stmt;
            END IF;

            SET @query = CONCAT('UPDATE T SET `', _Item,'` = 1 WHERE ID = ', _TransactionID );
            PREPARE stmt FROM @query;
            EXECUTE stmt;

        END LOOP _Fetch;
    CLOSE _Cursor;

END