DROP PROCEDURE IF EXISTS Apriori;

DELIMITER $$

CREATE PROCEDURE Apriori(IN _Utente VARCHAR(16), IN _soglia FLOAT, OUT _N_Item INT)
BEGIN

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Creazione della tabella Contenente le transazioni, ogni transazione contiene avvii paralleli di dispositivi da parte di un utente
	DECLARE _TS_Inizio_1 DATETIME;
    DECLARE _TS_Inizio_2 DATETIME;
    DECLARE _TS_Fine_1 DATETIME;
    DECLARE _Dispositivo_1 VARCHAR(255);
	DECLARE _Dispositivo_2 VARCHAR(255);
    DECLARE _Consecutivo BOOLEAN DEFAULT FALSE; # Flag che indica che si sta leggendo una sequenza di dispositivi avviati parallelamente
    DECLARE _N_Transazione INTEGER DEFAULT 0; # Contine di volta in volta il numero di transazioni create
    DECLARE _N_Dispositivi INTEGER DEFAULT (SELECT COUNT(*) FROM Dispositivo);
    
    DECLARE _Finito INTEGER DEFAULT 0;
    
    DECLARE _Cursore CURSOR FOR 
	SELECT PD.TS_Inizio, PD.Dispositivo, PD.TS_Fine, LEAD(PD.TS_Inizio, 1) OVER w AS Inizio_Disp_Succ, LEAD(PD.Dispositivo, 1) OVER w AS Disp_Succ
	FROM Pianificazione_Dispositivo PD
	WHERE PD.Utente = _Utente COLLATE utf8mb4_0900_ai_ci
	WINDOW w AS(ORDER BY PD.TS_Inizio);
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET _Finito = 1;

	SET SESSION group_concat_max_len = 5000;    
    
	SELECT GROUP_CONCAT(CONCAT('`', D.Nome, '`', ' BOOLEAN DEFAULT FALSE')) INTO @pivot_table
	FROM Dispositivo D
    ORDER BY D.Nome;
    
    SET @pivot_table = concat('CREATE TEMPORARY TABLE T(', ' ID INT AUTO_INCREMENT PRIMARY KEY, ',  @pivot_table, ' )ENGINE = InnoDB DEFAULT CHARSET = latin1;');
                          
	DROP TABLE IF EXISTS T;
	PREPARE create_Table_Transazione from @pivot_table; 
	EXECUTE create_Table_Transazione;
    
    OPEN _Cursore;
    
    _Preleva: LOOP
		FETCH _Cursore INTO _TS_Inizio_1, _Dispositivo_1, _TS_Fine_1, _TS_Inizio_2, _Dispositivo_2;
		
        IF _Finito = 1 THEN
			LEAVE _Preleva;
		END IF;
        
        # Se i dispositivi letti sono avviati parallelamente
        IF _TS_Inizio_2 <= _TS_Fine_1 THEN
	
			# Se i dispositivi letti non sono parallei a nessun dispositivio precedentemente letto
			IF _Consecutivo IS FALSE THEN
				# Viene creata una nuova transazione avente ID univoco e tutti glialtri valori FALSE
				INSERT INTO T VALUES();
                
                # Viene incrementato il numero di trnsazioni create così da poterlo utilizzare nel passo successivo
				SET _N_Transazione = _N_Transazione + 1;
			END IF;
        
			SET _Consecutivo = TRUE;
			SET @q = CONCAT('UPDATE `T` SET `',_Dispositivo_1,'` = TRUE, `',_Dispositivo_2,'` = TRUE WHERE `ID` = ',_N_Transazione);
            PREPARE nuova_transazione from @q; 
			EXECUTE nuova_transazione;
            
		ELSE 
			SET _Consecutivo = FALSE;
        END IF;
        
	END LOOP _Preleva;
    CLOSE _Cursore;
    
    IF NOT EXISTS(SELECT * FROM T) THEN 
		SIGNAL SQLSTATE '45000' 
        SET MESSAGE_TEXT =  'Tabella delle transazioni vuota';
    END IF;
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	
    #Visualizza la tabella delle transazioni
    #SELECT * FROM T;
    
    SET @_k = 1;
	REPEAT    
		CALL Supporto(_soglia, @_k);
        
		IF EXISTS(SELECT * FROM Large_Itemset_list) THEN
        
			SET @_table_name = CONCAT('Large_itemset_',@_k);
        
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# Le regole associative con relativo supporto vengono salvate ad ogni passo in una TEMPORAY TABLE
    # dedicata per poi poterle usare successivamente nel calcolo della confidenza
			SET @_query = CONCAT('DROP TEMPORARY TABLE IF EXISTS ',@_table_name,';');
            PREPARE _statement FROM @_query;
            EXECUTE _statement;
            
            SET @_query = CONCAT('CREATE TEMPORARY TABLE ',@_table_name,' AS (SELECT * FROM  Large_Itemset_list);');
            PREPARE _statement FROM @_query;
            EXECUTE _statement;
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

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
			# _k viene in caso di uscita dal ciclo decrementato in modo da avere memorizzato il numero di item per ogni k-item,
            # sarà utile in seguito per accedere all'ultima tabella creata contenente la lsita finale dei large k-itemset
			SET @_k = @_k-1;
		END IF;
	UNTIL @_k > _N_Dispositivi OR NOT EXISTS(SELECT * FROM Large_Itemset_list)
    END REPEAT;   
    
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
	# La TEMPORARY TABEL Large_itemset_(_confidence_step) viene elimina non essendo più necessare nei passi successivi
        SET @_query = CONCAT('DROP TEMPORARY TABLE IF EXISTS Large_itemset_',@_confidence_step);
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
    
    SET _N_Item = @_k;
    
    #SELECT * FROM _Result;

    DROP TABLE IF EXISTS C;
    DROP TABLE IF EXISTS Large_Itemset_list;
    DROP TABLE IF EXISTS T;
END $$

DELIMITER ;