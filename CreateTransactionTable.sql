DROP PROCEDURE IF EXISTS CreateTransactionTable;

DELIMITER $$

CREATE PROCEDURE CreateTransactionTable()
BEGIN

    DECLARE _Item VARCHAR(50);
    DECLARE _MemberNumber INT;
    DECLARE _Date DATE;

    DECLARE _PrecMember INT DEFAULT NULL;
    DECLARE _PrecDate DATE DEFAULT NULL;
    DECLARE _PrecItem VARCHAR(255);

    DECLARE _TransactionID INT DEFAULT 0;

    DECLARE _End BOOL DEFAULT FALSE;

    DECLARE _Cursor CURSOR FOR
    SELECT * FROM Groceries_Dataset D ORDER BY D.MemberNumber, D._Date;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET _End = TRUE;

    SET group_concat_max_len = 1000000;

# CREAZIONE DELLA TABELLA DELLE TRANSAZIONI
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    DROP TABLE IF EXISTS T ;

    SET @TransactionTable = 'CREATE TABLE T (ID INT PRIMARY KEY';

    SELECT GROUP_CONCAT( CONCAT(ItemDescription, ' BOOLEAN DEFAULT FALSE') SEPARATOR ' ')
    FROM (
        SELECT DISTINCT CONCAT(',`', ItemDescription, '`') AS ItemDescription
        FROM Groceries_Dataset
    ) AS D
    INTO @tmp;

    SET @TransactionTable = CONCAT(@TransactionTable, @tmp, ')');

    PREPARE stmt FROM @TransactionTable;
    EXECUTE stmt;
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    OPEN _Cursor;
        _Fetch: LOOP
            FETCH _Cursor INTO _MemberNumber, _Date, _Item;

            IF _End IS TRUE THEN
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

END $$

DELIMITER ;