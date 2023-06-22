DROP PROCEDURE IF EXISTS Apriori;

DELIMITER $$

CREATE PROCEDURE Apriori(IN transactionTableName VARCHAR(16), IN supportThreshold FLOAT, IN maxLength INT)
BEGIN


    DECLARE _N_Transaction INT DEFAULT 0;
    DECLARE _N_Item INT DEFAULT (
                                    SELECT COUNT(COLUMN_NAME)-1
                                    FROM INFORMATION_SCHEMA.COLUMNS
                                    WHERE TABLE_SCHEMA = Database() AND
                                          TABLE_NAME = transactionTableName
                                );
    DECLARE _ItemName VARCHAR(255);
    DECLARE _End INTEGER DEFAULT 0;

    DECLARE _Cursor CURSOR FOR
        SELECT *
        FROM (
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = Database() AND
                  TABLE_NAME = transactionTableName
        ) AS D WHERE D.COLUMN_NAME <> 'ID';

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET _End = 1;

    IF maxLength <= 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'maxLength must be greater than 0';
    END IF;

    IF supportThreshold <= 0 OR supportThreshold > 1 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'supportThreshold must be a number in the range ]0,1]';
    END IF;

    # Conteggio delle transazioni
    SET @_query = CONCAT('SELECT COUNT(*) INTO @_tmp FROM `',transactionTableName,'`');
    PREPARE _statement FROM @_query;
    EXECUTE _statement;

    SET _N_Transaction = @_tmp;
    SET @_table_name = 'Large_ItemSet_1';

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Creazione della tabella per memorizzare i k-itemset che hanno supporto superiore alla soglia
        DROP TABLE IF EXISTS Large_ItemSet_1;
        CREATE TABLE Large_ItemSet_1(
            Item_1 VARCHAR(255),
            Support FLOAT
        );
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Passo di Pruning, si scorre l'insieme C degli itemset candidati (in qeuesto caso al passo 1 è ugiale alla lista degli item) con un cursore
    #e se ne calcola il supporto, se supera la soglia il k-itemset viene inserito nella tabell Large_Itemset_list contentenete
    #il supporto calcolato per ogni k-itemset
        OPEN _Cursor;
        _Fetch: LOOP
            FETCH _Cursor INTO _ItemName;

            IF _End = 1 THEN
                LEAVE _Fetch;
            END IF;

            # Calcolo supporto itemset
            SET @_query = CONCAT('SELECT COUNT(*) INTO @_support FROM `',transactionTableName,'` WHERE `',_ItemName,'` IS TRUE');
            PREPARE _statement FROM @_query;
            EXECUTE _statement;

            #Un k-itemset viene inserito nella tabella Large_Itemset_list solo se supera il supporto scelto
            IF @_support/_N_Transaction >= supportThreshold THEN
                SET @_query = CONCAT('INSERT INTO `Large_ItemSet_1` VALUES(''', _ItemName ,''',',@_support/_N_Transaction,')');
                PREPARE _statement FROM @_query;
                EXECUTE _statement;
            END IF;

        END LOOP _Fetch;
        CLOSE _Cursor;
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    SET @_k = 1;
apriori_step:
    WHILE (@_k < maxLength) AND (@_k < _N_Item) DO

        SET @_k = @_k+1;

	    SET @_table_name = CONCAT('Large_ItemSet_',@_k);

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# Lista delle condizioni per il passo di join
		SET @_whereCondition = '';
		SET @i = 1;
		WHILE @i < @_K DO
			IF @i = @_k-1 THEN
				SET @_whereCondition = CONCAT(@_whereCondition, ' L1.Item_',@i,' <> L2.Item_',@i);
            ELSE
				SET @_whereCondition = CONCAT(@_whereCondition, ' L1.Item_',@i,' = L2.Item_',@i, ' AND ');
            END IF;
			SET @i = @i+1;
		END WHILE;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# Lista degli Item da proiettare dopo il Join
        SET @_select = '';
		SET @i = 1;
		WHILE @i < @_k DO
			SET @_select = CONCAT(@_select,'''`'',L1.Item_',@i,',''`,'',');
			SET @i = @i+1;
		END WHILE;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# Creazione della tabella C contenente l'insieme degli itemset candidati composti da k item, nella tabella è presente ogni disposizione senza ripetizione
    # di k elementi di ogni (k-1)-itemset in modo da avere alla fine già ogni regola associativa e calcolarne successivamente la confidenza
		DROP TABLE IF EXISTS C;
		SET @_query = CONCAT('CREATE TABLE C AS ( SELECT CONCAT(',@_select,'''`'',L2.Item_',@_k-1,',''`'') AS K_Item FROM `Large_ItemSet_',@_k-1,'` L1 CROSS JOIN `Large_ItemSet_',@_k-1,'` L2 WHERE', @_whereCondition ,' );');
        PREPARE _statement FROM @_query;
		EXECUTE _statement;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# Creazione della tabella per memorizzare i k-itemset che hanno supporto maggiore o uguale alla soglia scelta
	    SET @_query = CONCAT('DROP TABLE IF EXISTS `',@_table_name,'`');
	    PREPARE _statement FROM @_query;
		EXECUTE _statement;

        SET @_query = CONCAT('CREATE TABLE `',@_table_name,'`(');

        SET @i = 1;
        WHILE @i <= @_k DO
            SET @_query = CONCAT(@_query, ' Item_', @i,' VARCHAR(255),');
            SET @i = @i+1;
        END WHILE;

        SET @_query = CONCAT(@_query,' Support FLOAT DEFAULT 0);');
        PREPARE _statement FROM @_query;
        EXECUTE _statement;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# Passo di Pruning, si scorre l'insieme C degli itemset candidati con un cursore e se ne calcola il supporto, se è maggiore o uguale alla soglia
	# il k-itemset viene inserito nella tabella Large_Itemset_list contentenete il supporto calcolato per ogni k-itemset

		BEGIN
			DECLARE _CursorData TEXT;
            DECLARE _End INTEGER DEFAULT 0;
            DECLARE _Cursor CURSOR FOR SELECT K_Item FROM C ;
            DECLARE CONTINUE HANDLER FOR NOT FOUND SET _End = 1;

            OPEN _Cursor;

            _Fetch: LOOP
				FETCH _Cursor INTO _CursorData;

				IF _End = 1 THEN
					LEAVE _Fetch;
				END IF;

                SET @_tmp = _CursorData;
				SET @_query = CONCAT('SELECT SUM(IF(',REPLACE(@_tmp,',',' AND '),', 1, 0)) INTO @_support FROM `T`');
                PREPARE _statement FROM @_query;
				EXECUTE _statement;

				#Un k-itemset viene inserito nella tabella Large_Itemset_list solo se supera il supporto scelto
				IF @_support/_N_Transaction >= supportThreshold THEN
				    SET @_query = CONCAT('INSERT INTO `',@_table_name,'` VALUES(', REPLACE(_CursorData,'`','''') ,',',@_support/_N_Transaction,')');
				    PREPARE _statement FROM @_query;
                    EXECUTE _statement;
				END IF;

			END LOOP _Fetch;
			CLOSE _Cursor;
        END ;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

	    SET @_query = CONCAT('SELECT NOT EXISTS(SELECT * FROM  ',@_table_name,') INTO @_empty_result');
        PREPARE _statement FROM @_query;
        EXECUTE _statement;

        IF @_empty_result THEN

			SET @_query = CONCAT('DROP TABLE IF EXISTS `',@_table_name,'`');
	        PREPARE _statement FROM @_query;
		    EXECUTE _statement;

			# _k viene decrementato in caso di uscita dal ciclo in modo da avere memorizzato il numero di item per ogni k-item,
            # sarà utile in seguito per accedere all'ultima tabella creata contenente la lista finale dei large k-itemset
			SET @_k = @_k-1;
			SET @_table_name = CONCAT('Large_ItemSet_',@_k);

			LEAVE apriori_step;
        END IF;

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Calcolo della confidenza per ogni regola associativa

    IF maxLength > 1 THEN
        # SQL_SAFE_UPDATES viene disabilitato dovendo successivamente fare un update senza clausola WHERE
        SET SQL_SAFE_UPDATES = 0;

        SET @_confidence_step = 1;
        WHILE @_confidence_step < @_k DO

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Viene aggiunta una colonna Confidenza_n dove n indica la confidenza quando l'itemset X (antecedente) contiene n item
            SET @_query = CONCAT('ALTER TABLE ',@_table_name,' ADD COLUMN Confidence_',@_confidence_step,' FLOAT DEFAULT 0;');
            PREPARE _statement FROM @_query;
            EXECUTE _statement;
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Lista delle condizioni per il join
            SET @_joinConditions = '';
            SET @i = 1;
            WHILE @i <= @_confidence_step DO
                SET @_joinConditions = CONCAT(@_joinConditions, 'Item_',@i,IF(@i = @_confidence_step,'',','));
                SET @i = @i+1;
            END WHILE;
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Confidenza_n assume il valore di SUPP(X U Y)/SUPP(X) per ogni record,
        # SUPP(X U Y) viene preso dall'ultima TEMPORARY TABLE Large_itemset generata contenente il supporto per ogni k-itemset
        # e SUPP(X) invece viene preso da Large_itemset_(_confidence_step) TEMPORARY TABLE generata nei passi precedenti
        # contenete il supporto di X
            SET @_query = CONCAT('UPDATE ',@_table_name,' XY INNER JOIN Large_ItemSet_',@_confidence_step,' X USING(',@_joinConditions,') SET Confidence_',@_confidence_step,' = XY.Support/X.Support;');
            PREPARE _statement FROM @_query;
            EXECUTE _statement;
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            SET @_confidence_step = @_confidence_step+1;
        END WHILE;

        SET SQL_SAFE_UPDATES = 1;

    END IF;
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    END WHILE apriori_step;

    DROP TABLE IF EXISTS C;

END $$

DELIMITER ;