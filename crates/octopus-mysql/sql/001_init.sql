-- Octopus schema for MySQL 8.4 (InnoDB). Statements are separated by lines holding only $$ (stored procedures
-- contain semicolons). The schema prefix `octopus_` is replaced by the configured one (MySQL has no schemas inside a
-- database: tables are named <prefix>grp, <prefix>event, ...).
--
-- The same model as the PostgreSQL backend: positions (epoch, seq) per source, a cursor per source, stored events
-- not yet applied, skip markers, epoch ends, leases and members. Batches arrive as one JSON parameter, bodies in
-- base64 (FROM_BASE64). Names use utf8mb4_bin: comparison and ordering by bytes, as in Rust. Times are UTC
-- (UTC_TIMESTAMP(6)), on the database clock. Sessions run READ COMMITTED (no gap locks; set by the client).
--
-- MySQL constraints that shape the procedures:
-- - a TEMPORARY table may appear only once per statement, so intermediate results are copied between temporary
--   tables instead of joined with themselves;
-- - no partial indexes: skip markers are found through an index on (sid, skip_to);
-- - DDL is not transactional: the migration runs under GET_LOCK and every statement is idempotent.

CREATE TABLE IF NOT EXISTS octopus_grp (
    name        VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
    created_at  DATETIME(6) NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
    -- a source stopped by a missing event for this long (ms) is moved past it. NULL: the dispatchers' default;
    -- negative: never (alert only).
    gap_skip_ms INT NULL
) ENGINE = InnoDB
$$
CREATE TABLE IF NOT EXISTS octopus_source (
    source   VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
    id       BIGINT NOT NULL AUTO_INCREMENT,
    grp      VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    epoch    BIGINT NOT NULL,
    last_seq BIGINT NOT NULL DEFAULT 0,
    UNIQUE KEY source_id (id),
    KEY source_grp (grp, source, id, epoch, last_seq)
) ENGINE = InnoDB
$$
-- Stored events not yet applied (clustered on the key: one source's events are adjacent).
CREATE TABLE IF NOT EXISTS octopus_event (
    sid         BIGINT NOT NULL,
    epoch       BIGINT NOT NULL,
    seq         BIGINT NOT NULL,
    grp         VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    enqueued_at DATETIME(6) NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
    attempts    INT NOT NULL DEFAULT 0,
    not_before  DATETIME(6) NULL,
    -- a skip marker: the source declares seq ..= skip_to (this epoch) lost; never handed to a worker
    skip_to     BIGINT NULL,
    -- a hash of the body (computed by the dispatcher): a repeat with a different body is a conflict
    body_hash   BIGINT NULL,
    body        LONGBLOB NOT NULL,
    PRIMARY KEY (sid, epoch, seq),
    KEY event_markers (sid, skip_to)
) ENGINE = InnoDB
$$
CREATE TABLE IF NOT EXISTS octopus_epoch_end (
    sid        BIGINT NOT NULL,
    epoch      BIGINT NOT NULL,
    last_seq   BIGINT NOT NULL,
    next_epoch BIGINT NOT NULL,
    PRIMARY KEY (sid, epoch)
) ENGINE = InnoDB
$$
CREATE TABLE IF NOT EXISTS octopus_skipped (
    sid      BIGINT NOT NULL,
    epoch    BIGINT NOT NULL,
    from_seq BIGINT NOT NULL,
    to_seq   BIGINT NULL,               -- NULL: the rest of the epoch
    reason   VARCHAR(400) NOT NULL,
    at       DATETIME(6) NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
    PRIMARY KEY (sid, epoch, from_seq)
) ENGINE = InnoDB
$$
CREATE TABLE IF NOT EXISTS octopus_late (
    sid         BIGINT NOT NULL,
    epoch       BIGINT NOT NULL,
    seq         BIGINT NOT NULL,
    body        LONGBLOB NOT NULL,
    received_at DATETIME(6) NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
    PRIMARY KEY (sid, epoch, seq)
) ENGINE = InnoDB
$$
CREATE TABLE IF NOT EXISTS octopus_member (
    member_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
    address   VARCHAR(400) NOT NULL,
    heartbeat DATETIME(6) NOT NULL
) ENGINE = InnoDB
$$
CREATE TABLE IF NOT EXISTS octopus_lease (
    grp        VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
    owner      VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    epoch      BIGINT NOT NULL DEFAULT 1,
    expires_at DATETIME(6) NOT NULL
) ENGINE = InnoDB
$$
-- After cursors moved (the caller holds the source rows locked; their ids are in the temporary table
-- octopus_tmp_ids): a cursor at its epoch's end moves on to the next epoch; skip markers at or below the cursor are
-- recorded as skipped; every event at or below the cursor is deleted (two bounded range deletes per source: a delete
-- on the source id alone would walk, and lock, the source's newer rows too).
DROP PROCEDURE IF EXISTS octopus_settle
$$
CREATE PROCEDURE octopus_settle()
BEGIN
    DECLARE n INT DEFAULT 1;
    IF EXISTS (SELECT 1 FROM octopus_epoch_end x JOIN octopus_tmp_ids i ON i.id = x.sid) THEN
        WHILE n > 0 DO
            UPDATE octopus_source s JOIN octopus_tmp_ids i ON i.id = s.id
              JOIN octopus_epoch_end x ON x.sid = s.id AND x.epoch = s.epoch
               SET s.epoch = x.next_epoch, s.last_seq = 0
             WHERE s.last_seq >= x.last_seq AND x.next_epoch > s.epoch;
            SET n = ROW_COUNT();
        END WHILE;
    END IF;
    INSERT IGNORE INTO octopus_skipped (sid, epoch, from_seq, to_seq, reason)
    SELECT e.sid, e.epoch, e.seq, e.skip_to, 'declared by the source'
      FROM octopus_tmp_ids i JOIN octopus_source s ON s.id = i.id
      JOIN octopus_event e FORCE INDEX (event_markers) ON e.sid = s.id AND e.skip_to IS NOT NULL
     WHERE e.epoch < s.epoch OR (e.epoch = s.epoch AND e.seq <= s.last_seq);
    DELETE e FROM octopus_tmp_ids i JOIN octopus_source s ON s.id = i.id
      JOIN octopus_event e ON e.sid = s.id AND e.epoch = s.epoch AND e.seq <= s.last_seq;
    DELETE e FROM octopus_tmp_ids i JOIN octopus_source s ON s.id = i.id
      JOIN octopus_event e ON e.sid = s.id AND e.epoch < s.epoch;
END
$$
-- Locks the sources named in octopus_tmp_keys (in key order: no deadlocks between callers) and puts their ids in
-- octopus_tmp_ids.
DROP PROCEDURE IF EXISTS octopus_lock_sources
$$
CREATE PROCEDURE octopus_lock_sources()
BEGIN
    DELETE FROM octopus_tmp_ids;
    INSERT INTO octopus_tmp_ids (id)
    SELECT s.id FROM octopus_source s JOIN octopus_tmp_keys k ON k.s = s.source ORDER BY s.source FOR UPDATE OF s;
END
$$
-- Per-session scratch tables (temporary: private to the connection, kept across calls, emptied before use).
DROP PROCEDURE IF EXISTS octopus_scratch
$$
CREATE PROCEDURE octopus_scratch()
BEGIN
    CREATE TEMPORARY TABLE IF NOT EXISTS octopus_tmp_ids (id BIGINT NOT NULL PRIMARY KEY) ENGINE = InnoDB;
    CREATE TEMPORARY TABLE IF NOT EXISTS octopus_tmp_keys (
        s VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY) ENGINE = InnoDB;
    CREATE TEMPORARY TABLE IF NOT EXISTS octopus_tmp_u (
        i INT NOT NULL PRIMARY KEY, g VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
        s VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, e BIGINT, q BIGINT, b LONGBLOB, h BIGINT, k BIGINT,
        pe BIGINT, pl BIGINT) ENGINE = InnoDB;
    CREATE TEMPORARY TABLE IF NOT EXISTS octopus_tmp_x (
        i INT NOT NULL PRIMARY KEY, g VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, sid BIGINT,
        sgrp VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, ce BIGINT, cs BIGINT, e BIGINT, q BIGINT,
        h BIGINT, k BIGINT, mate BIGINT, existed TINYINT, ex_hash BIGINT) ENGINE = InnoDB;
    CREATE TEMPORARY TABLE IF NOT EXISTS octopus_tmp_st (
        i INT NOT NULL PRIMARY KEY, code SMALLINT, miss BIGINT) ENGINE = InnoDB;
END
$$
-- Publish. p_j: [{"i","g","s","e","q","b": base64 body,"h","k","pe","pl"}]; p_known = 1: every source and group is
-- registered already. Result rows (i, code, missing), codes as in the PostgreSQL backend: 0 stored, 1 duplicate,
-- 2 the source belongs to another group, 3 stored and it is the source's head now, 4 conflict, 5 stale epoch,
-- 6 late. "Stored" is decided against the rows present before the insert: a concurrent publish of the same position
-- may make both report it stored (harmless: the dispatchers deduplicate).
DROP PROCEDURE IF EXISTS octopus_publish
$$
CREATE PROCEDURE octopus_publish(IN p_j JSON, IN p_known TINYINT)
BEGIN
    CALL octopus_scratch();
    DELETE FROM octopus_tmp_u;
    INSERT INTO octopus_tmp_u
    SELECT j.i, j.g, j.s, j.e, j.q, FROM_BASE64(j.b), j.h, j.k, j.pe, j.pl
      FROM JSON_TABLE(p_j, '$[*]' COLUMNS (
            i INT PATH '$.i', g VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$.g',
            s VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$.s', e BIGINT PATH '$.e',
            q BIGINT PATH '$.q', b LONGTEXT PATH '$.b', h BIGINT PATH '$.h', k BIGINT PATH '$.k',
            pe BIGINT PATH '$.pe', pl BIGINT PATH '$.pl')) j;
    START TRANSACTION;
    IF p_known = 0 THEN
        INSERT IGNORE INTO octopus_grp (name) SELECT DISTINCT u.g FROM octopus_tmp_u u ORDER BY u.g;
        INSERT IGNORE INTO octopus_source (source, grp, epoch)
        SELECT u.s, MIN(u.g), MIN(u.e) FROM octopus_tmp_u u GROUP BY u.s ORDER BY u.s;
    END IF;
    -- declared ends of previous epochs (the first declaration wins)
    IF EXISTS (SELECT 1 FROM octopus_tmp_u WHERE pe IS NOT NULL) THEN
        DELETE FROM octopus_tmp_keys;
        INSERT IGNORE INTO octopus_tmp_keys (s) SELECT u.s FROM octopus_tmp_u u WHERE u.pe IS NOT NULL;
        CALL octopus_lock_sources();
        INSERT IGNORE INTO octopus_epoch_end (sid, epoch, last_seq, next_epoch)
        SELECT src.id, u.pe, u.pl, u.e FROM octopus_tmp_u u JOIN octopus_source src ON src.source = u.s
         WHERE u.pe IS NOT NULL AND u.pl IS NOT NULL AND u.pe < u.e AND u.pl >= 0
         ORDER BY src.id, u.pe, u.e;
        -- the cursor may already stand at the declared end: move it on now
        CALL octopus_settle();
    END IF;
    DELETE FROM octopus_tmp_x;
    INSERT INTO octopus_tmp_x (i, g, sid, sgrp, ce, cs, e, q, h, k, mate, existed, ex_hash)
    SELECT u.i, u.g, src.id, src.grp, src.epoch, src.last_seq, u.e, u.q, u.h, u.k,
           -- the highest position covered by a batchmate below this one (same source and epoch)
           MAX(COALESCE(u.k, u.q)) OVER (PARTITION BY src.id, u.e ORDER BY u.q ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
           -- a row stored before this publish (decides "stored" vs "duplicate", and conflicts)
           (y.sid IS NOT NULL), y.body_hash
      FROM octopus_tmp_u u JOIN octopus_source src ON src.source = u.s
      LEFT JOIN octopus_event y ON y.sid = src.id AND y.epoch = u.e AND y.seq = u.q;
    -- Never acknowledge an event that was not processed: a source not found here (the caller believed it registered,
    -- or its registration is not visible yet) aborts the batch, and the caller retries with registration. (Reporting
    -- such events as duplicates lost them: matrix-31, 4,987 first events acknowledged and never stored.)
    IF (SELECT COUNT(*) FROM octopus_tmp_x) < (SELECT COUNT(*) FROM octopus_tmp_u) THEN
        ROLLBACK;
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'octopus: unknown source in publish batch';
    END IF;
    -- (rows stored by a concurrent publish meanwhile are ignored by the key)
    INSERT IGNORE INTO octopus_event (sid, epoch, seq, grp, body, body_hash, skip_to)
    SELECT x.sid, x.e, x.q, x.g, u.b, x.h, x.k FROM octopus_tmp_x x JOIN octopus_tmp_u u ON u.i = x.i
     WHERE x.sgrp = x.g AND x.existed = 0 AND (x.e > x.ce OR (x.e = x.ce AND x.q > x.cs))
     ORDER BY x.sid, x.e, x.q;          -- one insertion order for everyone: no deadlocks between publishes
    DELETE FROM octopus_tmp_st;
    INSERT INTO octopus_tmp_st (i, code, miss)
    SELECT x.i,
           CASE WHEN x.sgrp <> x.g THEN 2
                WHEN x.existed = 0 AND (x.e > x.ce OR (x.e = x.ce AND x.q > x.cs)) AND x.e = x.ce AND x.q = x.cs + 1 THEN 3
                WHEN x.existed = 0 AND (x.e > x.ce OR (x.e = x.ce AND x.q > x.cs)) THEN 0
                WHEN x.existed = 1 AND x.ex_hash IS NOT NULL AND x.ex_hash <> x.h THEN 4
                WHEN x.existed = 1 THEN 1
                WHEN EXISTS (SELECT 1 FROM octopus_skipped k WHERE k.sid = x.sid AND k.epoch = x.e
                                AND x.q >= k.from_seq AND (k.to_seq IS NULL OR x.q <= k.to_seq)) THEN 6
                WHEN x.e < x.ce AND NOT EXISTS (SELECT 1 FROM octopus_epoch_end d
                                WHERE d.sid = x.sid AND d.epoch = x.e AND x.q <= d.last_seq) THEN 5
                ELSE 1 END,
           CASE WHEN x.e < x.ce THEN 0
                WHEN x.e = x.ce AND x.q = x.cs + 1 THEN 0
                WHEN x.mate IS NOT NULL AND x.mate >= x.q - 1 THEN 0
                ELSE GREATEST(0, x.q - 1 - GREATEST(
                    CASE WHEN x.e = x.ce THEN x.cs ELSE 0 END,
                    COALESCE((SELECT COALESCE(y.skip_to, y.seq) FROM octopus_event y
                               WHERE y.sid = x.sid AND y.epoch = x.e AND y.seq < x.q ORDER BY y.seq DESC LIMIT 1), 0),
                    COALESCE(x.mate, 0)))
           END
      FROM octopus_tmp_x x;
    INSERT IGNORE INTO octopus_late (sid, epoch, seq, body)
    SELECT x.sid, x.e, x.q, u.b FROM octopus_tmp_st st JOIN octopus_tmp_x x ON x.i = st.i JOIN octopus_tmp_u u ON u.i = st.i
     WHERE st.code = 6;
    COMMIT;
    SELECT u.i, COALESCE(st.code, 1) AS code, CASE WHEN st.code IN (0, 1, 3) THEN st.miss ELSE 0 END AS missing
      FROM octopus_tmp_u u LEFT JOIN octopus_tmp_st st ON st.i = u.i ORDER BY u.i;
END
$$
-- Moves cursors forward (never back) to the highest position given per source. p_j: [{"s","e","q"}]. The caller
-- holds the sources locked.
DROP PROCEDURE IF EXISTS octopus_move_cursors
$$
CREATE PROCEDURE octopus_move_cursors(IN p_j JSON)
BEGIN
    UPDATE octopus_source src
      JOIN (SELECT j.s, j.e, j.q FROM (
                SELECT j0.s, j0.e, j0.q, ROW_NUMBER() OVER (PARTITION BY j0.s ORDER BY j0.e DESC, j0.q DESC) AS rn
                  FROM JSON_TABLE(p_j, '$[*]' COLUMNS (s VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$.s',
                                                       e BIGINT PATH '$.e', q BIGINT PATH '$.q')) j0) j
             WHERE j.rn = 1) w ON w.s = src.source
       SET src.epoch = w.e, src.last_seq = w.q
     WHERE w.e > src.epoch OR (w.e = src.epoch AND w.q > src.last_seq);
END
$$
-- Persist cursors advanced by "done" reports (in bulk): each only moves forward; events at or below are deleted.
DROP PROCEDURE IF EXISTS octopus_advance
$$
CREATE PROCEDURE octopus_advance(IN p_j JSON)
BEGIN
    CALL octopus_scratch();
    DELETE FROM octopus_tmp_keys;
    INSERT IGNORE INTO octopus_tmp_keys (s)
    SELECT j.s FROM JSON_TABLE(p_j, '$[*]' COLUMNS (s VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$.s')) j;
    START TRANSACTION;
    CALL octopus_lock_sources();
    CALL octopus_move_cursors(p_j);
    CALL octopus_settle();
    COMMIT;
END
$$
-- Remove events reported done and return each source's next event if it is stored and due. p_j: [{"i","s","e","q"}].
-- Result rows (i, rc, next_epoch, next_seq, next_body, next_attempts, next_enqueued_us, next_skip_to): rc 0 removed
-- now, 1 already removed.
DROP PROCEDURE IF EXISTS octopus_remove_next
$$
CREATE PROCEDURE octopus_remove_next(IN p_j JSON)
BEGIN
    CALL octopus_scratch();
    DELETE FROM octopus_tmp_keys;
    INSERT IGNORE INTO octopus_tmp_keys (s)
    SELECT j.s FROM JSON_TABLE(p_j, '$[*]' COLUMNS (s VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$.s')) j;
    START TRANSACTION;
    CALL octopus_lock_sources();
    DELETE FROM octopus_tmp_st;
    INSERT INTO octopus_tmp_st (i, code, miss)
    SELECT j.i, CASE WHEN s.epoch < j.e OR (s.epoch = j.e AND s.last_seq < j.q) THEN 0 ELSE 1 END, 0
      FROM JSON_TABLE(p_j, '$[*]' COLUMNS (i INT PATH '$.i', s VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$.s',
                                           e BIGINT PATH '$.e', q BIGINT PATH '$.q')) j
      LEFT JOIN octopus_source s ON s.source = j.s;
    CALL octopus_move_cursors(p_j);
    CALL octopus_settle();
    COMMIT;
    SELECT j.i, st.code, e.epoch, e.seq, e.body, e.attempts,
           CAST(TIMESTAMPDIFF(MICROSECOND, '1970-01-01', e.enqueued_at) AS SIGNED), e.skip_to
      FROM JSON_TABLE(p_j, '$[*]' COLUMNS (i INT PATH '$.i', s VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$.s')) j
      JOIN octopus_tmp_st st ON st.i = j.i
      JOIN octopus_source s ON s.source = j.s
      LEFT JOIN octopus_event e ON e.sid = s.id AND e.epoch = s.epoch AND e.seq = s.last_seq + 1
                               AND (e.not_before IS NULL OR e.not_before <= UTC_TIMESTAMP(6))
     ORDER BY j.i;
END
$$
-- Commit (apply-hook mode). p_j: [{"i","s","e","q","f": base64 effect}]. Applies each event that is exactly its
-- source's head (sources locked in order), calls the apply hook procedure once with the applied events as JSON
-- [{"s","q","f"}] (by name, through a prepared statement), and deletes every row at or below the new cursors.
-- Result rows (i, code): 0 applied, 1 duplicate, 2 gap.
DROP PROCEDURE IF EXISTS octopus_commit_many
$$
CREATE PROCEDURE octopus_commit_many(IN p_j JSON, IN p_hook VARCHAR(400))
BEGIN
    DECLARE done INT DEFAULT 0;
    DECLARE v_i INT; DECLARE v_s VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
    DECLARE v_e BIGINT; DECLARE v_q BIGINT; DECLARE v_f LONGTEXT; DECLARE v_ce BIGINT; DECLARE v_cs BIGINT;
    DECLARE v_applied JSON DEFAULT JSON_ARRAY();
    DECLARE c CURSOR FOR
        SELECT j.i, j.s, j.e, j.q, j.f FROM JSON_TABLE(p_j, '$[*]' COLUMNS (
            i INT PATH '$.i', s VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$.s',
            e BIGINT PATH '$.e', q BIGINT PATH '$.q', f LONGTEXT PATH '$.f')) j ORDER BY j.s, j.e, j.q;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
    CALL octopus_scratch();
    DELETE FROM octopus_tmp_keys;
    INSERT IGNORE INTO octopus_tmp_keys (s)
    SELECT j.s FROM JSON_TABLE(p_j, '$[*]' COLUMNS (s VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$.s')) j;
    DELETE FROM octopus_tmp_st;
    START TRANSACTION;
    CALL octopus_lock_sources();
    OPEN c;
    l: LOOP
        FETCH c INTO v_i, v_s, v_e, v_q, v_f;
        IF done = 1 THEN LEAVE l; END IF;
        SET v_ce = NULL, v_cs = NULL;
        SELECT epoch, last_seq INTO v_ce, v_cs FROM octopus_source WHERE source = v_s;
        IF v_ce IS NULL THEN
            INSERT INTO octopus_tmp_st VALUES (v_i, 2, 0);
        ELSEIF v_ce = v_e AND v_cs = v_q - 1 THEN
            UPDATE octopus_source SET last_seq = v_q WHERE source = v_s;
            INSERT INTO octopus_tmp_st VALUES (v_i, 0, 0);
            SET v_applied = JSON_ARRAY_APPEND(v_applied, '$', JSON_OBJECT('s', v_s, 'q', v_q, 'f', v_f));
        ELSEIF v_ce > v_e OR (v_ce = v_e AND v_cs >= v_q) THEN
            INSERT INTO octopus_tmp_st VALUES (v_i, 1, 0);
        ELSE
            INSERT INTO octopus_tmp_st VALUES (v_i, 2, 0);
        END IF;
        SET done = 0;
    END LOOP;
    CLOSE c;
    IF p_hook IS NOT NULL AND JSON_LENGTH(v_applied) > 0 THEN
        IF p_hook NOT REGEXP '^[A-Za-z0-9_]+$' THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'apply hook: a procedure name of [A-Za-z0-9_]';
        END IF;
        SET @octopus_hook_j = v_applied;
        SET @octopus_hook_sql = CONCAT('CALL `', p_hook, '`(@octopus_hook_j)');
        PREPARE octopus_hook FROM @octopus_hook_sql;
        EXECUTE octopus_hook;
        DEALLOCATE PREPARE octopus_hook;
    END IF;
    CALL octopus_settle();
    COMMIT;
    SELECT i, code FROM octopus_tmp_st ORDER BY i;
END
$$
-- Give up positions of a source stopped by a missing event, if its cursor still stands at (p_epoch, p_after).
-- p_to: skip p_after + 1 ..= p_to. NULL: close the epoch (at its declared end if there is one, else where the cursor
-- stands) and move on to the next stored epoch. One result row (new_epoch, new_seq, from_seq, to_seq), NULLs if
-- nothing changed. Only what is missing now is given up: never an event stored meanwhile.
DROP PROCEDURE IF EXISTS octopus_skip
$$
CREATE PROCEDURE octopus_skip(IN p_source VARCHAR(255), IN p_epoch BIGINT, IN p_after BIGINT, IN p_to BIGINT, IN p_reason VARCHAR(400))
proc: BEGIN
    DECLARE v_id BIGINT; DECLARE v_ce BIGINT; DECLARE v_cs BIGINT; DECLARE v_nxt BIGINT; DECLARE v_upto BIGINT;
    SET v_upto = p_to;
    CALL octopus_scratch();
    START TRANSACTION;
    SELECT id, epoch, last_seq INTO v_id, v_ce, v_cs FROM octopus_source
     WHERE source = CONVERT(p_source USING utf8mb4) COLLATE utf8mb4_bin FOR UPDATE;
    IF v_id IS NULL OR v_ce <> p_epoch OR v_cs <> p_after THEN
        COMMIT; SELECT NULL, NULL, NULL, NULL; LEAVE proc;
    END IF;
    IF v_upto IS NULL THEN
        SELECT x.last_seq INTO v_upto FROM octopus_epoch_end x WHERE x.sid = v_id AND x.epoch = p_epoch AND x.last_seq > p_after;
    END IF;
    SELECT MIN(e.seq) INTO v_nxt FROM octopus_event e WHERE e.sid = v_id AND e.epoch = p_epoch AND e.seq > p_after;
    IF v_nxt IS NOT NULL THEN
        SET v_upto = LEAST(COALESCE(v_upto, v_nxt - 1), v_nxt - 1);
    END IF;
    SET v_nxt = NULL;
    IF v_upto IS NOT NULL THEN
        IF v_upto <= p_after THEN
            COMMIT; SELECT NULL, NULL, NULL, NULL; LEAVE proc;
        END IF;
        UPDATE octopus_source SET last_seq = v_upto WHERE id = v_id;
        INSERT IGNORE INTO octopus_skipped (sid, epoch, from_seq, to_seq, reason) VALUES (v_id, p_epoch, p_after + 1, v_upto, p_reason);
    ELSE
        SELECT MIN(e.epoch) INTO v_nxt FROM octopus_event e WHERE e.sid = v_id AND e.epoch > p_epoch;
        IF v_nxt IS NULL THEN
            COMMIT; SELECT NULL, NULL, NULL, NULL; LEAVE proc;
        END IF;
        INSERT INTO octopus_epoch_end (sid, epoch, last_seq, next_epoch) VALUES (v_id, p_epoch, p_after, v_nxt)
            ON DUPLICATE KEY UPDATE last_seq = VALUES(last_seq), next_epoch = VALUES(next_epoch);
        INSERT IGNORE INTO octopus_skipped (sid, epoch, from_seq, to_seq, reason) VALUES (v_id, p_epoch, p_after + 1, NULL, p_reason);
    END IF;
    DELETE FROM octopus_tmp_ids;
    INSERT INTO octopus_tmp_ids VALUES (v_id);
    CALL octopus_settle();
    COMMIT;
    SELECT epoch, last_seq, p_after + 1, v_upto FROM octopus_source WHERE id = v_id;
END
$$
-- Rows left at or below their cursor by a publish/removal race on a source that has not moved since. Housekeeping
-- that must not hold up the hot path: candidates by a plain read, then deleted by key, skipping locked rows.
DROP PROCEDURE IF EXISTS octopus_purge_applied
$$
CREATE PROCEDURE octopus_purge_applied(IN p_limit INT)
BEGIN
    DECLARE n BIGINT DEFAULT 0;
    CREATE TEMPORARY TABLE IF NOT EXISTS octopus_tmp_purge (sid BIGINT, epoch BIGINT, seq BIGINT, PRIMARY KEY (sid, epoch, seq)) ENGINE = InnoDB;
    DELETE FROM octopus_tmp_purge;
    INSERT INTO octopus_tmp_purge
    SELECT e.sid, e.epoch, e.seq FROM octopus_event e JOIN octopus_source s ON s.id = e.sid
     WHERE e.epoch < s.epoch OR (e.epoch = s.epoch AND e.seq <= s.last_seq) LIMIT p_limit;
    START TRANSACTION;
    DELETE e FROM octopus_event e
      JOIN (SELECT e2.sid, e2.epoch, e2.seq FROM octopus_event e2 JOIN octopus_tmp_purge c
              ON c.sid = e2.sid AND c.epoch = e2.epoch AND c.seq = e2.seq FOR UPDATE OF e2 SKIP LOCKED) d
        ON d.sid = e.sid AND d.epoch = e.epoch AND d.seq = e.seq
      JOIN octopus_source s ON s.id = e.sid
     WHERE e.epoch < s.epoch OR (e.epoch = s.epoch AND e.seq <= s.last_seq);
    SET n = ROW_COUNT();
    COMMIT;
    SELECT n;
END
$$
