DROP PROCEDURE IF EXISTS Supporto;

DELIMITER $$

CREATE PROCEDURE Supporto(IN _soglia DOUBLE, IN _step INT)
BEGIN

    DECLARE _N_Transazioni INT DEFAULT (SELECT COUNT(*) FROM T);
    
    # Con il primo step viene viene calcolato il supporto di ogni item e vengono inseriti nella tabella Large_Itemset_list solo quelli
    # che con supporto maggiore o uguale al supporto minimo scelto
    IF _step = 1 THEN
		BEGIN 
			DECLARE _Nome_Dispositivo VARCHAR(255);
            DECLARE _Finito INTEGER DEFAULT 0;
			DECLARE _Cursore CURSOR FOR SELECT D.Nome FROM Dispositivo D;
            DECLARE CONTINUE HANDLER FOR NOT FOUND SET _Finito = 1;
            
		# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
		# Creazione della tabella per memorizzare i k-itemset che hanno supporto superiore alla soglia
            DROP TABLE IF EXISTS Large_Itemset_list;
            CREATE TABLE Large_Itemset_list(
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
				IF @_supporto/_N_Transazioni >= _soglia THEN
					SET @_query = CONCAT('INSERT INTO `Large_Itemset_list` VALUES(''', _Nome_Dispositivo ,''',',@_supporto/_N_Transazioni,')');                    
                    PREPARE _statement FROM @_query;
					EXECUTE _statement;
				END IF;
				
			END LOOP _Preleva;
			CLOSE _Cursore;
		# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        END ;
    ELSE

	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# Lista delle condizioni per il passo di join
		SET @_condizione_where = '';
		SET @i = 1;
		WHILE @i < _step DO
			IF @i = _step-1 THEN
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
		WHILE @i < _step DO
			SET @_parametro_select = CONCAT(@_parametro_select,'''`'',L1.Item',@i,',''`,'',');
			SET @i = @i+1;
		END WHILE;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# Creazione della tabella C contenente l'insieme degli itemset candidati composti da k item, nella tabella è presente ogni disposizione senza ripetizione
    # di k elementi di ogni (k-1)-itemset in modo da avere alla fine già ogni regola associativa e calcolarne successivamente la confidenza
		DROP TABLE IF EXISTS C;
		SET @_query = CONCAT('CREATE TABLE C AS ( SELECT CONCAT(',@_parametro_select,'''`'',L2.Item',_step-1,',''`'') AS K_Item FROM Large_Itemset_list L1 CROSS JOIN Large_Itemset_list L2 WHERE', @_condizione_where ,' );');        
        PREPARE _statement FROM @_query;
		EXECUTE _statement;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# Creazione della tabella per memorizzare i k-itemset che hanno supporto maggiore o uguale alla soglia scelta
			DROP TABLE IF EXISTS Large_Itemset_list;
			
			SET @_query = 'CREATE TABLE Large_Itemset_list(';
			
			SET @i = 1;
			WHILE @i <= _step DO
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
				IF @_supporto/_N_Transazioni >= _soglia THEN
					SET @_query = CONCAT('INSERT INTO `Large_Itemset_list` VALUES(', REPLACE(_Fetch_Cursore,'`','''') ,',',@_supporto/_N_Transazioni,')');
                    PREPARE _statement FROM @_query;
					EXECUTE _statement;
				END IF;
				
			END LOOP _Preleva;
			CLOSE _Cursore;
        END ;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -	
    END IF;
END $$

DELIMITER ;