DROP PROCEDURE IF EXISTS Apriori;

DELIMITER $$

CREATE PROCEDURE Apriori(IN tabellaTransazioni VARCHAR(16), IN sogliaSupporto FLOAT)
BEGIN

    DECLARE _N_Transazioni INT DEFAULT 0;
    DECLARE _N_Item INT DEFAULT (
                                    SELECT COUNT(COLUMN_NAME)-1
                                    FROM INFORMATION_SCHEMA.COLUMNS
                                    WHERE TABLE_SCHEMA = Database() AND
                                          TABLE_NAME = tabellaTransazioni
                                );
    DECLARE _Nome_Dispositivo VARCHAR(255);
    DECLARE _Finito INTEGER DEFAULT 0;

    DECLARE _Cursore CURSOR FOR
        SELECT *
        FROM (
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = Database() AND
                  TABLE_NAME = tabellaTransazioni
        ) AS D WHERE D.COLUMN_NAME <> 'ID';

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET _Finito = 1;

    # Conteggio delle transazioni
    SET @_query = CONCAT('SELECT COUNT(*) INTO @_tmp FROM `',tabellaTransazioni,'`');
    PREPARE _statement FROM @_query;
    EXECUTE _statement;

    SET _N_Transazioni = @_tmp;

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Creazione della tabella per memorizzare i k-itemset che hanno supporto superiore alla soglia
        DROP TABLE IF EXISTS Large_Itemset_1;
        CREATE TABLE Large_Itemset_1(
            Item1 VARCHAR(255),
            Supporto DECIMAL(3,2)
        );
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Passo di Pruning, si scorre l'insieme C degli itemset candidati (in qeuesto caso al passo 1 è ugiale alla lista degli item) con un cursore
    #e se ne calcola il supporto, se supera la soglia il k-itemset viene inserito nella tabell Large_Itemset_list contentenete
    #il supporto calcolato per ogni k-itemset
        OPEN _Cursore;
        _Preleva: LOOP
            FETCH _Cursore INTO _Nome_Dispositivo;

            IF _Finito = 1 THEN
                LEAVE _Preleva;
            END IF;

            SET @_query = CONCAT('SELECT SUM(`',_Nome_Dispositivo,'`) INTO @_supporto FROM `T`');
            PREPARE _statement FROM @_query;
            EXECUTE _statement;

            #Un k-itemset viene inserito nella tabella Large_Itemset_list solo se supera il supporto scelto
            IF @_supporto/_N_Transazioni >= sogliaSupporto THEN
                SET @_query = CONCAT('INSERT INTO `Large_Itemset_1` VALUES(''', _Nome_Dispositivo ,''',',@_supporto/_N_Transazioni,')');
                PREPARE _statement FROM @_query;
                EXECUTE _statement;
            END IF;

        END LOOP _Preleva;
        CLOSE _Cursore;
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    SET @_k = 2;
apriori_step:
    WHILE @_k <= _N_Item DO

	    SET @_table_name = CONCAT('Large_itemset_',@_k);

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# Lista delle condizioni per il passo di join
		SET @_condizione_where = '';
		SET @i = 1;
		WHILE @i < @_K DO
			IF @i = @_k-1 THEN
				SET @_condizione_where = CONCAT(@_condizione_where, ' L1.Item',@i,' <> L2.Item',@i);
            ELSE
				SET @_condizione_where = CONCAT(@_condizione_where, ' L1.Item',@i,' = L2.Item',@i, ' AND ');
            END IF;
			SET @i = @i+1;
		END WHILE;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# Lista degli Item da proiettare dopo il Join
        SET @_parametro_select = '';
		SET @i = 1;
		WHILE @i < @_k DO
			SET @_parametro_select = CONCAT(@_parametro_select,'''`'',L1.Item',@i,',''`,'',');
			SET @i = @i+1;
		END WHILE;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# Creazione della tabella C contenente l'insieme degli itemset candidati composti da k item, nella tabella è presente ogni disposizione senza ripetizione
    # di k elementi di ogni (k-1)-itemset in modo da avere alla fine già ogni regola associativa e calcolarne successivamente la confidenza
		DROP TABLE IF EXISTS C;
		SET @_query = CONCAT('CREATE TABLE C AS ( SELECT CONCAT(',@_parametro_select,'''`'',L2.Item',@_k-1,',''`'') AS K_Item FROM `Large_itemset_',@_k-1,'` L1 CROSS JOIN `Large_itemset_',@_k-1,'` L2 WHERE', @_condizione_where ,' );');
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
            SET @_query = CONCAT(@_query, ' Item', @i,' VARCHAR(255),');
            SET @i = @i+1;
        END WHILE;

        SET @_query = CONCAT(@_query,'Supporto DECIMAL(3,2));');
        PREPARE _statement FROM @_query;
        EXECUTE _statement;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# Passo di Pruning, si scorre l'insieme C degli itemset candidati con un cursore e se ne calcola il supporto, se è maggiore o uguale alla soglia
	# il k-itemset viene inserito nella tabella Large_Itemset_list contentenete il supporto calcolato per ogni k-itemset
		BEGIN
			DECLARE _Fetch_Cursore TEXT;
            DECLARE _Finito INTEGER DEFAULT 0;
            DECLARE _Cursore CURSOR FOR SELECT K_Item FROM C ;
            DECLARE CONTINUE HANDLER FOR NOT FOUND SET _Finito = 1;

            OPEN _Cursore;

            _Preleva: LOOP
				FETCH _Cursore INTO _Fetch_Cursore;

				IF _Finito = 1 THEN
					LEAVE _Preleva;
				END IF;

                SET @_tmp = _Fetch_Cursore;
				SET @_query = CONCAT('SELECT SUM(IF(',REPLACE(@_tmp,',',' AND '),', 1, 0)) INTO @_supporto FROM `T`');
                PREPARE _statement FROM @_query;
				EXECUTE _statement;

				#Un k-itemset viene inserito nella tabella Large_Itemset_list solo se supera il supporto scelto
				IF @_supporto/_N_Transazioni >= sogliaSupporto THEN
					SET @_query = CONCAT('INSERT INTO `',@_table_name,'` VALUES(', REPLACE(_Fetch_Cursore,'`','''') ,',',@_supporto/_N_Transazioni,')');
                    PREPARE _statement FROM @_query;
					EXECUTE _statement;
				END IF;

			END LOOP _Preleva;
			CLOSE _Cursore;
        END ;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

	    SET @_query = CONCAT('SELECT NOT EXISTS(SELECT * FROM  ',@_table_name,') INTO @_empty_result');
        PREPARE _statement FROM @_query;
        EXECUTE _statement;

		IF NOT @_empty_result THEN

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Visualizza tabella supporti al passo @_k
		/*
            SET @_query = CONCAT('SELECT * FROM  ',@_table_name);    
            PREPARE _statement FROM @_query;
            EXECUTE _statement;
		 */
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        
			SET @_k = @_k+1;
		ELSE
			# _k viene decrementato in caso di uscita dal ciclo in modo da avere memorizzato il numero di item per ogni k-item,
            # sarà utile in seguito per accedere all'ultima tabella creata contenente la lista finale dei large k-itemset
			SET @_k = @_k-1;

			SET @_query = CONCAT('DROP TABLE IF EXISTS `',@_table_name,'`');
	        PREPARE _statement FROM @_query;
		    EXECUTE _statement;

			SET @_table_name = CONCAT('Large_itemset_',@_k);

			LEAVE apriori_step;
		END IF;
    END WHILE apriori_step;

    # SQL_SAFE_UPDATES viene disabilitato dovendo successivamente fare un update senza clausola WHERE
    SET SQL_SAFE_UPDATES = 0;

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Calcolo della confidenza per ogni regola associativa
    SET @_confidence_step = 1;
    WHILE @_confidence_step < @_k DO

	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# Viene aggiunta una colonna Confidenza_n dove n indica la confidenza quando l'itemset X (antecedente) contiene n item
        SET @_query = CONCAT('ALTER TABLE ',@_table_name,' ADD COLUMN Confidenza_',@_confidence_step,' DECIMAL(3,2) DEFAULT 0;');
		PREPARE _statement FROM @_query;
		EXECUTE _statement;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# Lista delle condizioni per il join
		SET @_parametro_join = '';
		SET @i = 1;
		WHILE @i <= @_confidence_step DO
			SET @_parametro_join = CONCAT(@_parametro_join, 'Item',@i,IF(@i = @_confidence_step,'',','));	
			SET @i = @i+1;
		END WHILE;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# Confidenza_n assume il valore di SUPP(X U Y)/SUPP(X) per ogni record,
    # SUPP(X U Y) viene preso dall'ultima TEMPORARY TABLE Large_itemset generata contenente il supporto per ogni k-itemset
    # e SUPP(X) invece viene preso da Large_itemset_(_confidence_step) TEMPORARY TABLE generata nei passi precedenti
    # contenete il supporto di X
		SET @_query = CONCAT('UPDATE ',@_table_name,' XY INNER JOIN Large_itemset_',@_confidence_step,' X USING(',@_parametro_join,') SET Confidenza_',@_confidence_step,' = XY.Supporto/X.Supporto;');
        PREPARE _statement FROM @_query;
		EXECUTE _statement;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# La TEMPORARY TABLE Large_itemset_(_confidence_step) viene elimina non essendo più necessare nei passi successivi
        SET @_query = CONCAT('DROP TABLE IF EXISTS Large_itemset_',@_confidence_step);
        PREPARE _statement FROM @_query;
		EXECUTE _statement;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

		SET @_confidence_step = @_confidence_step+1;
    END WHILE;
    
	SET SQL_SAFE_UPDATES = 1;

	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# Crea la tabella finale con supporto e confidenza per ogni regola associativa
		DROP TEMPORARY TABLE IF EXISTS _Result;
		SET @_query = CONCAT('CREATE TEMPORARY TABLE _Result AS (SELECT * FROM  ',@_table_name,' ORDER BY Supporto)'); 
		PREPARE _statement FROM @_query;
		EXECUTE _statement;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# Eliminazione della tabella non più necessaria
		SET @_query = CONCAT('DROP TEMPORARY TABLE IF EXISTS ',@_table_name); 
		PREPARE _statement FROM @_query;
		EXECUTE _statement;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    
    SELECT * FROM _Result;

    DROP TABLE IF EXISTS C;

END $$

DELIMITER ;