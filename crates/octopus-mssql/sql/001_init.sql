-- Octopus schema for SQL Server 2025 (17.x). Idempotent: safe to run on every start (under an application lock).
-- Batches are separated by lines holding only GO. The schema name `octopus` is replaced by the configured one.
--
-- The same model as the PostgreSQL backend (see crates/octopus-pg/sql/001_init.sql): positions (epoch, seq) per
-- source, a cursor per source, stored events not yet applied, skip markers, epoch ends, leases and members.
-- Batches arrive as one JSON parameter (SQL Server has no array parameters); bodies are base64 in the JSON.
-- Names use a binary UTF-8 collation: comparison and ordering by bytes, as in Rust and in the PostgreSQL backend.
-- Strings from JSON are read as nvarchar and converted through that collation (CAST(x COLLATE ..._UTF8 AS
-- varchar)): read as varchar in the database's default code page, non-Latin characters would turn into '?'.
-- Times are UTC (SYSUTCDATETIME), on the database clock.

-- Filtered indexes need these on, and procedures keep the settings they were created with.
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO
IF SCHEMA_ID('octopus') IS NULL EXEC('CREATE SCHEMA octopus');
GO

IF OBJECT_ID('octopus.grp') IS NULL
CREATE TABLE octopus.grp (
    name        varchar(256) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL,
    created_at  datetime2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
    -- a source stopped by a missing event for this long (ms) is moved past it. NULL: the dispatchers' default;
    -- negative: never (alert only).
    gap_skip_ms int NULL,
    CONSTRAINT grp_pkey PRIMARY KEY (name) WITH (IGNORE_DUP_KEY = ON)
);
GO

IF OBJECT_ID('octopus.source') IS NULL
CREATE TABLE octopus.source (
    source   varchar(256) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL,
    id       bigint IDENTITY(1, 1) NOT NULL,
    grp      varchar(256) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL,
    epoch    bigint NOT NULL,
    last_seq bigint NOT NULL DEFAULT 0,
    CONSTRAINT source_pkey PRIMARY KEY (source) WITH (IGNORE_DUP_KEY = ON),
    CONSTRAINT source_id UNIQUE (id)
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'source_grp' AND object_id = OBJECT_ID('octopus.source'))
    CREATE INDEX source_grp ON octopus.source (grp, source) INCLUDE (id, epoch, last_seq) WITH (ALLOW_PAGE_LOCKS = OFF);
GO

-- Stored events not yet applied. Rows are inserted and deleted at the event rate: no lock escalation (a table lock
-- would stop every publish).
IF OBJECT_ID('octopus.event') IS NULL
CREATE TABLE octopus.event (
    sid         bigint NOT NULL,
    epoch       bigint NOT NULL,
    seq         bigint NOT NULL,
    grp         varchar(256) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL,
    enqueued_at datetime2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
    attempts    int NOT NULL DEFAULT 0,
    not_before  datetime2(6) NULL,
    -- a skip marker: the source declares seq ..= skip_to (this epoch) lost; never handed to a worker
    skip_to     bigint NULL,
    -- a hash of the body (computed by the dispatcher): a repeat with a different body is a conflict
    body_hash   bigint NULL,
    body        varbinary(max) NOT NULL,
    CONSTRAINT event_pkey PRIMARY KEY CLUSTERED (sid, epoch, seq) WITH (IGNORE_DUP_KEY = ON)
);
GO
-- Row locks only on the hot tables: a page lock taken by a delete or an update would make every insert into that page
-- wait (matrix-27: LCK_M_IX waits dominated), and no escalation to table locks.
ALTER TABLE octopus.event SET (LOCK_ESCALATION = DISABLE);
ALTER TABLE octopus.source SET (LOCK_ESCALATION = DISABLE);
ALTER INDEX event_pkey ON octopus.event SET (ALLOW_PAGE_LOCKS = OFF);
ALTER INDEX source_pkey ON octopus.source SET (ALLOW_PAGE_LOCKS = OFF);
ALTER INDEX source_id ON octopus.source SET (ALLOW_PAGE_LOCKS = OFF);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'event_markers' AND object_id = OBJECT_ID('octopus.event'))
    CREATE INDEX event_markers ON octopus.event (sid) WHERE skip_to IS NOT NULL;
GO

IF OBJECT_ID('octopus.epoch_end') IS NULL
CREATE TABLE octopus.epoch_end (
    sid        bigint NOT NULL,
    epoch      bigint NOT NULL,
    last_seq   bigint NOT NULL,
    next_epoch bigint NOT NULL,
    CONSTRAINT epoch_end_pkey PRIMARY KEY (sid, epoch) WITH (IGNORE_DUP_KEY = ON)
);
GO

IF OBJECT_ID('octopus.skipped') IS NULL
CREATE TABLE octopus.skipped (
    sid      bigint NOT NULL,
    epoch    bigint NOT NULL,
    from_seq bigint NOT NULL,
    to_seq   bigint NULL,               -- NULL: the rest of the epoch
    reason   nvarchar(400) NOT NULL,
    at       datetime2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT skipped_pkey PRIMARY KEY (sid, epoch, from_seq) WITH (IGNORE_DUP_KEY = ON)
);
GO

IF OBJECT_ID('octopus.late') IS NULL
CREATE TABLE octopus.late (
    sid         bigint NOT NULL,
    epoch       bigint NOT NULL,
    seq         bigint NOT NULL,
    body        varbinary(max) NOT NULL,
    received_at datetime2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT late_pkey PRIMARY KEY (sid, epoch, seq) WITH (IGNORE_DUP_KEY = ON)
);
GO

IF OBJECT_ID('octopus.member') IS NULL
CREATE TABLE octopus.member (
    member_id varchar(256) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL CONSTRAINT member_pkey PRIMARY KEY,
    address   nvarchar(400) NOT NULL,
    heartbeat datetime2(6) NOT NULL
);
GO

IF OBJECT_ID('octopus.lease') IS NULL
CREATE TABLE octopus.lease (
    grp        varchar(256) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL,
    owner      varchar(256) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL,
    epoch      bigint NOT NULL DEFAULT 1,
    expires_at datetime2(6) NOT NULL,
    CONSTRAINT lease_pkey PRIMARY KEY (grp) WITH (IGNORE_DUP_KEY = ON)
);
GO

-- Source ids passed between procedures.
IF TYPE_ID('octopus.idlist') IS NULL
    CREATE TYPE octopus.idlist AS TABLE (id bigint NOT NULL PRIMARY KEY);
GO

-- After cursors moved: a cursor that reached its epoch's end moves on to the next epoch; then every event at or
-- below the cursors is deleted (skip markers among them are recorded as skipped). The caller holds the source rows
-- locked.
CREATE OR ALTER PROCEDURE octopus.settle @ids octopus.idlist READONLY
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM octopus.epoch_end x JOIN @ids i ON i.id = x.sid)
    BEGIN
        WHILE 1 = 1
        BEGIN
            UPDATE s SET epoch = x.next_epoch, last_seq = 0
              FROM @ids i JOIN octopus.source s WITH (ROWLOCK, FORCESEEK) ON s.id = i.id
              JOIN octopus.epoch_end x ON x.sid = s.id AND x.epoch = s.epoch
             WHERE s.last_seq >= x.last_seq AND x.next_epoch > s.epoch
            OPTION (LOOP JOIN, FORCE ORDER);
            IF @@ROWCOUNT = 0 BREAK;
        END;
    END;
    IF EXISTS (SELECT 1 FROM octopus.event e WITH (INDEX (event_markers)) JOIN @ids i ON i.id = e.sid WHERE e.skip_to IS NOT NULL)
        INSERT INTO octopus.skipped (sid, epoch, from_seq, to_seq, reason)
        SELECT e.sid, e.epoch, e.seq, e.skip_to, N'declared by the source'
          FROM @ids i JOIN octopus.source s WITH (FORCESEEK) ON s.id = i.id
          JOIN octopus.event e WITH (FORCESEEK) ON e.sid = s.id
         WHERE e.skip_to IS NOT NULL AND (e.epoch < s.epoch OR (e.epoch = s.epoch AND e.seq <= s.last_seq))
        OPTION (LOOP JOIN, FORCE ORDER);
    DELETE e
      FROM @ids i JOIN octopus.source s WITH (FORCESEEK) ON s.id = i.id
      JOIN octopus.event e WITH (ROWLOCK, FORCESEEK) ON e.sid = s.id
     WHERE e.epoch < s.epoch OR (e.epoch = s.epoch AND e.seq <= s.last_seq)
    OPTION (LOOP JOIN, FORCE ORDER);
END;
GO

-- Publish. @j: [{"i":1,"g":group,"s":source,"e":epoch,"q":seq,"b":base64 body,"h":hash,"k":skip_to,"pe":prev_epoch,
-- "pl":prev_last_seq}]. @known = 1: every source and group of the batch is registered already.
-- Result rows (i, code, missing), codes as in the PostgreSQL backend: 0 stored, 1 duplicate, 2 the source belongs to
-- another group, 3 stored and it is the source's head now, 4 conflict, 5 stale epoch, 6 late.
CREATE OR ALTER PROCEDURE octopus.publish @j nvarchar(max), @known bit
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @u TABLE (i int PRIMARY KEY, g varchar(256) COLLATE Latin1_General_100_BIN2_UTF8, s varchar(256) COLLATE Latin1_General_100_BIN2_UTF8,
                      e bigint, q bigint, b varbinary(max), h bigint, k bigint, pe bigint, pl bigint);
    INSERT INTO @u
    SELECT i, CAST(g COLLATE Latin1_General_100_BIN2_UTF8 AS varchar(256)), CAST(s COLLATE Latin1_General_100_BIN2_UTF8 AS varchar(256)), e, q, BASE64_DECODE(b), h, k, pe, pl
      FROM OPENJSON(@j) WITH (i int '$.i', g nvarchar(256) '$.g', s nvarchar(256) '$.s', e bigint '$.e', q bigint '$.q',
                              b varchar(max) '$.b', h bigint '$.h', k bigint '$.k', pe bigint '$.pe', pl bigint '$.pl');
    BEGIN TRANSACTION;
    IF @known = 0
    BEGIN
        -- register only what is new (duplicates from concurrent publishes are ignored by the keys)
        INSERT INTO octopus.grp (name)
        SELECT DISTINCT u.g FROM @u u WHERE NOT EXISTS (SELECT 1 FROM octopus.grp x WHERE x.name = u.g);
        INSERT INTO octopus.source (source, grp, epoch)
        SELECT u.s, MIN(u.g), MIN(u.e) FROM @u u
         WHERE NOT EXISTS (SELECT 1 FROM octopus.source x WHERE x.source = u.s)
         GROUP BY u.s;
    END;
    -- declared ends of previous epochs (the first declaration wins)
    IF EXISTS (SELECT 1 FROM @u WHERE pe IS NOT NULL)
    BEGIN
        DECLARE @declared octopus.idlist;
        INSERT INTO octopus.epoch_end (sid, epoch, last_seq, next_epoch)
        OUTPUT inserted.sid INTO @declared
        SELECT d.id, d.pe, d.pl, d.e FROM (
            SELECT src.id, u.pe, u.pl, u.e, ROW_NUMBER() OVER (PARTITION BY src.id, u.pe ORDER BY u.e) AS rn
              FROM @u u JOIN octopus.source src ON src.source = u.s
             WHERE u.pe IS NOT NULL AND u.pl IS NOT NULL AND u.pe < u.e AND u.pl >= 0) d
         WHERE d.rn = 1;
        IF EXISTS (SELECT 1 FROM @declared)
        BEGIN
            -- the cursor may already stand at the declared end: move it on now (source rows locked in order)
            DECLARE @lk int;
            SELECT @lk = COUNT(*) FROM @declared d JOIN octopus.source s WITH (UPDLOCK, ROWLOCK, FORCESEEK) ON s.id = d.id
            OPTION (LOOP JOIN, FORCE ORDER);
            EXEC octopus.settle @declared;
        END;
    END;
    DECLARE @x TABLE (i int PRIMARY KEY, g varchar(256) COLLATE Latin1_General_100_BIN2_UTF8, sid bigint,
                      sgrp varchar(256) COLLATE Latin1_General_100_BIN2_UTF8, ce bigint, cs bigint, e bigint, q bigint,
                      b varbinary(max), h bigint, k bigint, mate bigint);
    INSERT INTO @x
    SELECT u.i, u.g, src.id, src.grp, src.epoch, src.last_seq, u.e, u.q, u.b, u.h, u.k,
           -- the highest position covered by a batchmate below this one (same source and epoch)
           MAX(COALESCE(u.k, u.q)) OVER (PARTITION BY src.id, u.e ORDER BY u.q ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
      FROM @u u JOIN octopus.source src WITH (FORCESEEK) ON src.source = u.s
    OPTION (LOOP JOIN);
    DECLARE @ins TABLE (sid bigint, epoch bigint, seq bigint, PRIMARY KEY (sid, epoch, seq));
    -- (rows already stored, by this or a concurrent publish, are ignored by the key and not in the output)
    INSERT INTO octopus.event (sid, epoch, seq, grp, body, body_hash, skip_to)
    OUTPUT inserted.sid, inserted.epoch, inserted.seq INTO @ins
    SELECT x.sid, x.e, x.q, x.g, x.b, x.h, x.k FROM @x x
     WHERE x.sgrp = x.g AND (x.e > x.ce OR (x.e = x.ce AND x.q > x.cs))
     ORDER BY x.sid, x.e, x.q;          -- one insertion order for everyone: no deadlocks between publishes
    DECLARE @st TABLE (i int PRIMARY KEY, sid bigint, e bigint, q bigint, b varbinary(max), code smallint, miss bigint);
    INSERT INTO @st
    SELECT x.i, x.sid, x.e, x.q, x.b,
           CASE WHEN x.sgrp <> x.g THEN 2
                WHEN n.sid IS NOT NULL AND x.e = x.ce AND x.q = x.cs + 1 THEN 3
                WHEN n.sid IS NOT NULL THEN 0
                WHEN ex.present = 1 AND ex.body_hash IS NOT NULL AND ex.body_hash <> x.h THEN 4
                WHEN ex.present = 1 THEN 1
                WHEN EXISTS (SELECT 1 FROM octopus.skipped k WHERE k.sid = x.sid AND k.epoch = x.e
                                AND x.q >= k.from_seq AND (k.to_seq IS NULL OR x.q <= k.to_seq)) THEN 6
                WHEN x.e < x.ce AND NOT EXISTS (SELECT 1 FROM octopus.epoch_end d
                                WHERE d.sid = x.sid AND d.epoch = x.e AND x.q <= d.last_seq) THEN 5
                ELSE 1 END,
           CASE WHEN x.e < x.ce THEN 0
                -- cheap answers first: the head, or the batchmate just below
                WHEN x.e = x.ce AND x.q = x.cs + 1 THEN 0
                WHEN x.mate IS NOT NULL AND x.mate >= x.q - 1 THEN 0
                ELSE (SELECT CASE WHEN m.v > 0 THEN m.v ELSE 0 END FROM (SELECT x.q - 1 - (SELECT MAX(c) FROM (VALUES
                        (CASE WHEN x.e = x.ce THEN x.cs ELSE 0 END),
                        ((SELECT TOP (1) COALESCE(y.skip_to, y.seq) FROM octopus.event y
                           WHERE y.sid = x.sid AND y.epoch = x.e AND y.seq < x.q ORDER BY y.seq DESC)),
                        (x.mate)) AS t(c)) AS v) m)
           END
      FROM @x x
      LEFT JOIN @ins n ON n.sid = x.sid AND n.epoch = x.e AND n.seq = x.q
      -- a row stored before this statement, looked up only for events not inserted now
      OUTER APPLY (SELECT TOP (1) 1 AS present, y.body_hash FROM octopus.event y
                    WHERE n.sid IS NULL AND y.sid = x.sid AND y.epoch = x.e AND y.seq = x.q) ex;
    INSERT INTO octopus.late (sid, epoch, seq, body)
    SELECT sid, e, q, b FROM @st WHERE code = 6;
    COMMIT;
    -- events of a source unknown here (registration failed) are reported as duplicates (code 1), as in PostgreSQL
    SELECT u.i, COALESCE(st.code, CAST(1 AS smallint)) AS code,
           CASE WHEN st.code IN (0, 1, 3) THEN st.miss ELSE 0 END AS missing
      FROM @u u LEFT JOIN @st st ON st.i = u.i
     ORDER BY u.i;
END;
GO

-- Commit (apply-hook mode): one transaction for a batch. @j: [{"i","s","e","q","f": base64 effect}]. Applies each
-- event that is exactly its source's head (sources locked in order), calls the apply hook procedure once with the
-- applied events as JSON [{"s","q","f"}], and deletes every row at or below the new cursors.
-- Result rows (i, code): 0 applied, 1 duplicate, 2 gap.
CREATE OR ALTER PROCEDURE octopus.commit_many @j nvarchar(max), @hook nvarchar(400)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @u TABLE (i int PRIMARY KEY, s varchar(256) COLLATE Latin1_General_100_BIN2_UTF8, e bigint, q bigint, f varchar(max));
    INSERT INTO @u SELECT i, CAST(s COLLATE Latin1_General_100_BIN2_UTF8 AS varchar(256)), e, q, f FROM OPENJSON(@j) WITH (i int '$.i', s nvarchar(256) '$.s', e bigint '$.e', q bigint '$.q', f varchar(max) '$.f');
    DECLARE @rc TABLE (i int PRIMARY KEY, code smallint);
    DECLARE @applied TABLE (n int IDENTITY PRIMARY KEY, s varchar(256) COLLATE Latin1_General_100_BIN2_UTF8, q bigint, f varchar(max));
    DECLARE @ids octopus.idlist;
    BEGIN TRANSACTION;
    DECLARE @k TABLE (s varchar(256) COLLATE Latin1_General_100_BIN2_UTF8 PRIMARY KEY WITH (IGNORE_DUP_KEY = ON));
    INSERT INTO @k SELECT s FROM @u;
    INSERT INTO @ids SELECT s.id FROM @k k JOIN octopus.source s WITH (UPDLOCK, ROWLOCK, FORCESEEK) ON s.source = k.s
    OPTION (LOOP JOIN, FORCE ORDER);
    DECLARE @i int, @s varchar(256), @e bigint, @q bigint, @f varchar(max), @ce bigint, @cs bigint;
    DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT i, s, e, q, f FROM @u ORDER BY s, e, q;
    OPEN c;
    FETCH NEXT FROM c INTO @i, @s, @e, @q, @f;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @ce = NULL, @cs = NULL;
        SELECT @ce = epoch, @cs = last_seq FROM octopus.source WHERE source = @s;
        IF @ce IS NULL
            INSERT INTO @rc VALUES (@i, 2);
        ELSE IF @ce = @e AND @cs = @q - 1
        BEGIN
            UPDATE octopus.source SET last_seq = @q WHERE source = @s;
            INSERT INTO @rc VALUES (@i, 0);
            INSERT INTO @applied (s, q, f) VALUES (@s, @q, @f);
        END
        ELSE IF @ce > @e OR (@ce = @e AND @cs >= @q)
            INSERT INTO @rc VALUES (@i, 1);
        ELSE
            INSERT INTO @rc VALUES (@i, 2);
        FETCH NEXT FROM c INTO @i, @s, @e, @q, @f;
    END;
    CLOSE c; DEALLOCATE c;
    IF @hook IS NOT NULL AND EXISTS (SELECT 1 FROM @applied)
    BEGIN
        IF OBJECT_ID(@hook, 'P') IS NULL THROW 50001, 'apply hook is not a procedure', 1;
        DECLARE @hj nvarchar(max) = (SELECT s, q, f FROM @applied ORDER BY n FOR JSON PATH);
        DECLARE @call nvarchar(max) = N'EXEC ' + QUOTENAME(OBJECT_SCHEMA_NAME(OBJECT_ID(@hook))) + N'.' + QUOTENAME(OBJECT_NAME(OBJECT_ID(@hook))) + N' @j';
        EXEC sp_executesql @call, N'@j nvarchar(max)', @j = @hj;
    END;
    EXEC octopus.settle @ids;
    COMMIT;
    SELECT i, code FROM @rc ORDER BY i;
END;
GO

-- Moves cursors forward (never back) to the highest position given per source; sources locked in order. @j:
-- [{"s","e","q"}]. Returns the ids of the sources touched in @ids (for settle and the caller).
CREATE OR ALTER PROCEDURE octopus.move_cursors @j nvarchar(max)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @w TABLE (s varchar(256) COLLATE Latin1_General_100_BIN2_UTF8 PRIMARY KEY, e bigint, q bigint);
    INSERT INTO @w
    SELECT s, e, q FROM (
        SELECT CAST(s COLLATE Latin1_General_100_BIN2_UTF8 AS varchar(256)) AS s, e, q, ROW_NUMBER() OVER (PARTITION BY s ORDER BY e DESC, q DESC) AS rn
          FROM OPENJSON(@j) WITH (s nvarchar(256) '$.s', e bigint '$.e', q bigint '$.q')) z
     WHERE rn = 1;
    UPDATE src SET epoch = w.e, last_seq = w.q
      FROM @w w JOIN octopus.source src WITH (ROWLOCK, FORCESEEK) ON src.source = w.s
     WHERE w.e > src.epoch OR (w.e = src.epoch AND w.q > src.last_seq)
    OPTION (LOOP JOIN, FORCE ORDER);
END;
GO

-- Remove events reported done (workers committed their own writes) and return each source's next event if it is
-- stored and due. @j: [{"i","s","e","q"}]. Result rows (i, rc, next_epoch, next_seq, next_body, next_attempts,
-- next_enqueued_us, next_skip_to): rc 0 removed now, 1 already removed. The cursor moves to the highest position
-- reported done (see the PostgreSQL backend for why that is sound).
CREATE OR ALTER PROCEDURE octopus.remove_next @j nvarchar(max)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @u TABLE (i int PRIMARY KEY, s varchar(256) COLLATE Latin1_General_100_BIN2_UTF8, e bigint, q bigint);
    INSERT INTO @u SELECT i, CAST(s COLLATE Latin1_General_100_BIN2_UTF8 AS varchar(256)), e, q FROM OPENJSON(@j) WITH (i int '$.i', s nvarchar(256) '$.s', e bigint '$.e', q bigint '$.q');
    DECLARE @ids octopus.idlist;
    DECLARE @rc TABLE (i int PRIMARY KEY, rc smallint);
    BEGIN TRANSACTION;
    DECLARE @k TABLE (s varchar(256) COLLATE Latin1_General_100_BIN2_UTF8 PRIMARY KEY WITH (IGNORE_DUP_KEY = ON));
    INSERT INTO @k SELECT s FROM @u;
    INSERT INTO @ids SELECT s.id FROM @k k JOIN octopus.source s WITH (UPDLOCK, ROWLOCK, FORCESEEK) ON s.source = k.s
    OPTION (LOOP JOIN, FORCE ORDER);
    INSERT INTO @rc
    SELECT u.i, CASE WHEN s.epoch < u.e OR (s.epoch = u.e AND s.last_seq < u.q) THEN 0 ELSE 1 END
      FROM @u u LEFT JOIN octopus.source s ON s.source = u.s;
    EXEC octopus.move_cursors @j;
    EXEC octopus.settle @ids;
    COMMIT;
    SELECT u.i, r.rc, e.epoch, e.seq, e.body, e.attempts, DATEDIFF_BIG(MICROSECOND, '1970-01-01', e.enqueued_at), e.skip_to
      FROM @u u JOIN @rc r ON r.i = u.i
      JOIN octopus.source s ON s.source = u.s
      LEFT JOIN octopus.event e ON e.sid = s.id AND e.epoch = s.epoch AND e.seq = s.last_seq + 1
                               AND (e.not_before IS NULL OR e.not_before <= SYSUTCDATETIME())
     ORDER BY u.i;
END;
GO

-- Persist cursors advanced by "done" reports (in bulk): each only moves forward; events at or below are deleted.
CREATE OR ALTER PROCEDURE octopus.advance @j nvarchar(max)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @ids octopus.idlist;
    BEGIN TRANSACTION;
    DECLARE @k TABLE (s varchar(256) COLLATE Latin1_General_100_BIN2_UTF8 PRIMARY KEY WITH (IGNORE_DUP_KEY = ON));
    INSERT INTO @k SELECT CAST(s COLLATE Latin1_General_100_BIN2_UTF8 AS varchar(256)) FROM OPENJSON(@j) WITH (s nvarchar(256) '$.s');
    INSERT INTO @ids SELECT s.id FROM @k k JOIN octopus.source s WITH (UPDLOCK, ROWLOCK, FORCESEEK) ON s.source = k.s
    OPTION (LOOP JOIN, FORCE ORDER);
    EXEC octopus.move_cursors @j;
    EXEC octopus.settle @ids;
    COMMIT;
END;
GO

-- Give up positions of a source stopped by a missing event, if its cursor still stands at (@epoch, @after).
-- @to: skip @after + 1 ..= @to. NULL: close the epoch (at its declared end if there is one, else where the cursor
-- stands) and move on to the next stored epoch. One result row (new_epoch, new_seq, from_seq, to_seq), NULLs if
-- nothing changed. Only what is missing now is given up: never an event stored meanwhile.
CREATE OR ALTER PROCEDURE octopus.skip @source nvarchar(256), @epoch bigint, @after bigint, @to bigint, @reason nvarchar(400)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @id bigint, @ce bigint, @cs bigint, @nxt bigint, @upto bigint = @to, @done bit = 0;
    BEGIN TRANSACTION;
    DECLARE @src varchar(256) = CAST(@source COLLATE Latin1_General_100_BIN2_UTF8 AS varchar(256));
    SELECT @id = id, @ce = epoch, @cs = last_seq FROM octopus.source WITH (UPDLOCK, ROWLOCK) WHERE source = @src;
    IF @id IS NULL OR @ce <> @epoch OR @cs <> @after
    BEGIN
        COMMIT; SELECT CAST(NULL AS bigint), CAST(NULL AS bigint), CAST(NULL AS bigint), CAST(NULL AS bigint); RETURN;
    END;
    IF @upto IS NULL
        SELECT @upto = x.last_seq FROM octopus.epoch_end x WHERE x.sid = @id AND x.epoch = @epoch AND x.last_seq > @after;
    SELECT @nxt = MIN(e.seq) FROM octopus.event e WHERE e.sid = @id AND e.epoch = @epoch AND e.seq > @after;
    IF @nxt IS NOT NULL
        SET @upto = CASE WHEN @upto IS NULL OR @upto > @nxt - 1 THEN @nxt - 1 ELSE @upto END;
    SET @nxt = NULL;
    IF @upto IS NOT NULL
    BEGIN
        IF @upto <= @after
        BEGIN
            COMMIT; SELECT CAST(NULL AS bigint), CAST(NULL AS bigint), CAST(NULL AS bigint), CAST(NULL AS bigint); RETURN;
        END;
        UPDATE octopus.source SET last_seq = @upto WHERE id = @id;
        INSERT INTO octopus.skipped (sid, epoch, from_seq, to_seq, reason) VALUES (@id, @epoch, @after + 1, @upto, @reason);
    END
    ELSE
    BEGIN
        SELECT @nxt = MIN(e.epoch) FROM octopus.event e WHERE e.sid = @id AND e.epoch > @epoch;
        IF @nxt IS NULL
        BEGIN
            COMMIT; SELECT CAST(NULL AS bigint), CAST(NULL AS bigint), CAST(NULL AS bigint), CAST(NULL AS bigint); RETURN;
        END;
        IF EXISTS (SELECT 1 FROM octopus.epoch_end WHERE sid = @id AND epoch = @epoch)
            UPDATE octopus.epoch_end SET last_seq = @after, next_epoch = @nxt WHERE sid = @id AND epoch = @epoch;
        ELSE
            INSERT INTO octopus.epoch_end (sid, epoch, last_seq, next_epoch) VALUES (@id, @epoch, @after, @nxt);
        INSERT INTO octopus.skipped (sid, epoch, from_seq, to_seq, reason) VALUES (@id, @epoch, @after + 1, NULL, @reason);
    END;
    DECLARE @ids octopus.idlist;
    INSERT INTO @ids VALUES (@id);
    EXEC octopus.settle @ids;
    COMMIT;
    SELECT epoch, last_seq, @after + 1, @upto FROM octopus.source WHERE id = @id;
END;
GO

-- Rows left at or below their cursor by a publish/removal race on a source that has not moved since.
CREATE OR ALTER PROCEDURE octopus.purge_applied @limit int
AS
BEGIN
    SET NOCOUNT ON;
    DELETE TOP (@limit) e
      FROM octopus.event e JOIN octopus.source s ON s.id = e.sid
     WHERE e.epoch < s.epoch OR (e.epoch = s.epoch AND e.seq <= s.last_seq);
    SELECT CAST(@@ROWCOUNT AS bigint);
END;
GO
