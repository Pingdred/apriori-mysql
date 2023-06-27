DROP PROCEDURE IF EXISTS Apriori;

DELIMITER $$

CREATE PROCEDURE Apriori(IN transactionTableName VARCHAR(16), IN supportThreshold FLOAT, IN itemSetSize INT)
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

# Transaction table cursor created by extracting the transaction table column names from the
# information schema
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    DECLARE _Cursor CURSOR FOR
        SELECT *
        FROM (
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = Database() AND
                  TABLE_NAME = transactionTableName
        ) AS D WHERE D.COLUMN_NAME <> 'ID';

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET _End = 1;
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# Input parameters boundaries check
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    IF itemSetSize <= 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'maxItemSetSize must be greater than 0';
    END IF;

    IF supportThreshold <= 0 OR supportThreshold > 1 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'supportThreshold must be a number in the range (0,1]';
    END IF;
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# Transaction counting
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    SET @_query = CONCAT('SELECT COUNT(*) INTO @_tmp FROM `',transactionTableName,'`');
    PREPARE _statement FROM @_query;
    EXECUTE _statement;

    SET _N_Transaction = @_tmp;
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# Creation of the table Large_ItemSet_1 to store the 1-ItemSet that have support greater than or
# equal to the chosen support threshold
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    DROP TABLE IF EXISTS Large_ItemSet_1;
    CREATE TABLE Large_ItemSet_1(
        Item_1 VARCHAR(255),
        Support FLOAT DEFAULT 0
    );

    SET @_table_name = 'Large_ItemSet_1';
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# First pruning step, the set C (at step 1, set C is the list of items) of the candidate ItemSets
# is scrolled and foreach itemSet its support is calculated, if it exceeds the chosen support threshold the 1-ItemSet
# is inserted in the Large_ItemSet_1 table containing the calculated support for each 1-ItemSet.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    OPEN _Cursor;
    _Fetch: LOOP
        FETCH _Cursor INTO _ItemName;

        IF _End = 1 THEN
            LEAVE _Fetch;
        END IF;

        # Calculate the support of the ItemSet
        SET @_query = CONCAT('SELECT COUNT(*) INTO @_support FROM `',transactionTableName,'` WHERE `',_ItemName,'` IS TRUE');
        PREPARE _statement FROM @_query;
        EXECUTE _statement;

        # Check if the ItemSet has support above the threshold
        IF @_support/_N_Transaction >= supportThreshold THEN
            # Inserting the ItemSet into the Large_ItemSet_1 table
            SET @_query = CONCAT('INSERT INTO `Large_ItemSet_1` VALUES(''', _ItemName ,''',',@_support/_N_Transaction,')');
            PREPARE _statement FROM @_query;
            EXECUTE _statement;
        END IF;

    END LOOP _Fetch;
    CLOSE _Cursor;
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# This loop does the following:
# - Join step: generates the set C of the candidate ItemSets;
# - Pruning step: selection of k-ItemSet with support above the threshold
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    SET @_k = 1;
apriori_step:
    WHILE (@_k < itemSetSize) AND (@_k < _N_Item) DO

        SET @_k = @_k+1;
	    SET @_table_name = CONCAT('Large_ItemSet_',@_k);

    # Creation of table C containing the candidate k-ItemSets. C has a single column K_ItemSet, which
	# contains the items in the following format: "`soda`,`rolls/buns`,`other vegetables`,`whole milk`".
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Generating the join conditions for the inner join
		SET @_whereCondition = '';
		SET @i = 1;
		WHILE @i < @_k DO

		    SET @_whereCondition = CONCAT(@_whereCondition, ' L1.Item_',@i,' <> L2.Item_',@_k);

			IF @i <> @_k-1 THEN
				SET @_whereCondition = CONCAT(@_whereCondition,' AND ');
            END IF;

			SET @i = @i+1;
		END WHILE;

        # The part of the select statement to project all columns of L1 excluding support
        SET @_select = '';
		SET @i = 1;
		WHILE @i < @_k DO
			SET @_select = CONCAT(@_select,'''`'',L1.Item_',@i,',''`,'',');
			SET @i = @i+1;
		END WHILE;

		DROP TABLE IF EXISTS C;
		SET @_query = CONCAT('CREATE TABLE C AS ( SELECT CONCAT(',@_select,'''`'',L2.Item_',@_k,',''`'') AS K_ItemSet FROM `Large_ItemSet_',@_k-1,'` L1 INNER JOIN ( SELECT DISTINCT Item_1 AS Item_',@_k,' FROM `Large_ItemSet_',@_k-1,'`) AS L2 ON', @_whereCondition ,' );');
        PREPARE _statement FROM @_query;
		EXECUTE _statement;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    # Creation of the table Large_ItemSet_k to store the k-ItemSet that have support greater than or
    # equal to the chosen threshold
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
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

    # Pruning step, the set C of the candidate ItemSets is scrolled and foreach ItemSet its support
    # is calculated, if greater than or equal the chosen support threshold the k-ItemSet is inserted
    # in the Large_ItemSet_k table containing the calculated support for each k-ItemSet.
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
		BEGIN
			DECLARE _CursorData TEXT;
            DECLARE _End INTEGER DEFAULT 0;
            DECLARE _Cursor CURSOR FOR SELECT K_ItemSet FROM C ;
            DECLARE CONTINUE HANDLER FOR NOT FOUND SET _End = 1;

            OPEN _Cursor;

            _Fetch: LOOP
				FETCH _Cursor INTO _CursorData;

				IF _End = 1 THEN
					LEAVE _Fetch;
				END IF;

				# Replacing commas with AND for the If clause
				SET @_tmp = _CursorData;
				SET @_tmp = REPLACE(@_tmp,',',' AND ');

 			    # Calculate the support of the ItemSet
				SET @_query = CONCAT('SELECT SUM(IF(',@_tmp,', 1, 0)) INTO @_support FROM `T`');
                PREPARE _statement FROM @_query;
				EXECUTE _statement;

                # Check if the ItemSet has support above the threshold
 				IF @_support/_N_Transaction >= supportThreshold THEN
				    # Inserting the ItemSet into the Large_ItemSet_1 table
				    SET @_query = CONCAT('INSERT INTO `',@_table_name,'` VALUES(', REPLACE(_CursorData,'`','''') ,',',0.01,')'); #@_support/_N_Transaction
				    PREPARE _statement FROM @_query;
                    EXECUTE _statement;
				END IF;

			END LOOP _Fetch;
			CLOSE _Cursor;
        END ;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    # Check if the table Large_ItemSet_K is empty
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	    SET @_query = CONCAT('SELECT NOT EXISTS(SELECT 1 FROM  ',@_table_name,') INTO @_empty_result');
        PREPARE _statement FROM @_query;
        EXECUTE _statement;

        IF @_empty_result THEN

            # Delete empty table
			SET @_query = CONCAT('DROP TABLE IF EXISTS `',@_table_name,'`');
	        PREPARE _statement FROM @_query;
		    EXECUTE _statement;

			SET @_k = @_k-1;

			LEAVE apriori_step;
        END IF;
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    END WHILE apriori_step;

# Calculate the confidence for each associative rule
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    SET @_table_name = CONCAT('Large_ItemSet_',@_k);

    IF itemSetSize > 1 AND @_k > 1 THEN
        # SQL_SAFE_UPDATES is disabled by having to do an UPDATE without a WHERE clause
        SET SQL_SAFE_UPDATES = 0;

        # Loop over the Large_ItemSet_k tables created to calculate the confidence for each k-ItemSet
        SET @_n = 1;
        WHILE @_n < @_k DO

        # A Confidence_n column is added to the Large_ItemSet_k table, n indicates the confidence
        # when ItemSet X (the antecedent of the rule) contains n items
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            SET @_query = CONCAT('ALTER TABLE ',@_table_name,' ADD COLUMN Confidence_',@_n,' FLOAT DEFAULT 0;');
            PREPARE _statement FROM @_query;
            EXECUTE _statement;
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        # Creating join conditions
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            SET @_joinConditions = '';
            SET @i = 1;
            WHILE @i <= @_n DO
                SET @_joinConditions = CONCAT(@_joinConditions, 'Item_',@i);

                IF @i <> @_n THEN
                    SET @_joinConditions = CONCAT(@_joinConditions,',');
                END IF;

                SET @i = @i+1;
            END WHILE;
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        # Confidence calculation for each associative rule.
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            SET @_query = CONCAT('UPDATE ',@_table_name,' XY INNER JOIN Large_ItemSet_',@_n,' X USING(',@_joinConditions,') SET Confidence_',@_n,' = XY.Support/X.Support;');
            PREPARE _statement FROM @_query;
            EXECUTE _statement;
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            SET @_n = @_n+1;
        END WHILE;

        SET SQL_SAFE_UPDATES = 1;

    END IF;
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    DROP TABLE IF EXISTS C;

END $$

DELIMITER ;