-- Octopus schema for PostgreSQL. Idempotent: safe to run on every start (under an advisory lock).
-- Octopus owns only these tables; application state lives in the application's own tables, written by its
-- apply hook inside the commit transaction.

CREATE SCHEMA IF NOT EXISTS octopus;

-- Workers never touch Octopus's tables (an earlier draft had a function for that).
DROP FUNCTION IF EXISTS octopus.complete(text, bigint);

-- Every group ever published to (the unit of ownership).
CREATE TABLE IF NOT EXISTS octopus.grp (
    name       text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- One row per source: its group (fixed by the first publish) and its cursor (last applied seq).
CREATE TABLE IF NOT EXISTS octopus.source (
    source   text PRIMARY KEY,
    grp      text NOT NULL,
    last_seq bigint NOT NULL DEFAULT 0
) WITH (fillfactor = 70);
CREATE INDEX IF NOT EXISTS source_grp ON octopus.source (grp, source);

-- Stored events not yet applied. A source's head is the row with seq = last_seq + 1.
CREATE TABLE IF NOT EXISTS octopus.event (
    source      text NOT NULL,
    seq         bigint NOT NULL,
    grp         text NOT NULL,
    enqueued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempts    int NOT NULL DEFAULT 0,
    not_before  timestamptz,
    body        bytea NOT NULL,
    PRIMARY KEY (source, seq)
) WITH (fillfactor = 90, autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 5000,
        autovacuum_vacuum_insert_scale_factor = 0, autovacuum_vacuum_insert_threshold = 20000,
        autovacuum_vacuum_cost_delay = 0);
-- Bodies are stored as they are: compressing multi-megabyte events on every insert costs more CPU than it saves.
ALTER TABLE octopus.event ALTER COLUMN body SET STORAGE EXTERNAL;

-- Dispatcher membership (heartbeats on the database clock).
CREATE TABLE IF NOT EXISTS octopus.member (
    member_id text PRIMARY KEY,
    address   text NOT NULL,
    heartbeat timestamptz NOT NULL
);

-- Group ownership. Only for efficiency and liveness: correctness never depends on it.
CREATE TABLE IF NOT EXISTS octopus.lease (
    grp        text PRIMARY KEY,
    owner      text NOT NULL,
    epoch      bigint NOT NULL DEFAULT 1,
    expires_at timestamptz NOT NULL
);

-- Publish: register groups and sources, then insert each event only if its seq is beyond the cursor
-- (an event at or below the cursor is already applied). Returns per event: 0 stored, 1 duplicate,
-- 2 the source belongs to another group, 3 stored and it is the source's head (seq = cursor + 1) right now.
--
-- A publish racing with the commit of the same (source, seq) can still insert a row at or below the cursor
-- (the insert waits for the commit's delete and then proceeds). Such a row is never a head; the next commit
-- of the source deletes it (range delete), and octopus.purge_applied() catches idle sources.
CREATE OR REPLACE FUNCTION octopus.publish(p_grp text[], p_source text[], p_seq bigint[], p_body bytea[])
RETURNS smallint[] LANGUAGE plpgsql AS $$
DECLARE
    r smallint[];
BEGIN
    -- register only what is new (an index probe each; ON CONFLICT would attempt every insert)
    INSERT INTO octopus.grp (name)
        SELECT DISTINCT g FROM unnest(p_grp) AS g
         WHERE NOT EXISTS (SELECT 1 FROM octopus.grp x WHERE x.name = g) ORDER BY 1
        ON CONFLICT DO NOTHING;
    INSERT INTO octopus.source (source, grp)
        SELECT DISTINCT ON (s) s, g FROM unnest(p_source, p_grp) AS u(s, g)
         WHERE NOT EXISTS (SELECT 1 FROM octopus.source x WHERE x.source = u.s) ORDER BY s
        ON CONFLICT DO NOTHING;
    WITH u AS (
        SELECT * FROM unnest(p_grp, p_source, p_seq, p_body) WITH ORDINALITY AS u(g, s, q, b, i)
    ), ins AS (
        INSERT INTO octopus.event (source, seq, grp, body)
        SELECT u.s, u.q, u.g, u.b
          FROM u JOIN octopus.source src ON src.source = u.s
         WHERE src.grp = u.g AND u.q > src.last_seq
         ORDER BY u.s, u.q              -- one insertion order for everyone: no deadlocks between publishes
        ON CONFLICT DO NOTHING
        RETURNING source, seq
    )
    SELECT array_agg(CASE WHEN src.grp <> u.g THEN 2
                          WHEN ins.source IS NOT NULL AND u.q = src.last_seq + 1 THEN 3
                          WHEN ins.source IS NOT NULL THEN 0
                          ELSE 1 END ORDER BY u.i)
      INTO r
      FROM u JOIN octopus.source src ON src.source = u.s
      LEFT JOIN ins ON ins.source = u.s AND ins.seq = u.q;
    RETURN r;
END $$;

-- Commit: one transaction for a batch. Locks the cursors in source order (no deadlocks between batches),
-- applies each event whose cursor is exactly seq - 1, calls the apply hook once with every applied event,
-- and deletes every row at or below the new cursors. Returns per commit: 0 applied, 1 duplicate, 2 gap.
-- p_hook: a function (text[], bigint[], bytea[]) or NULL; the cast to regproc rejects anything else.
CREATE OR REPLACE FUNCTION octopus.commit_many(p_source text[], p_seq bigint[], p_effect bytea[], p_hook text)
RETURNS smallint[] LANGUAGE plpgsql AS $$
DECLARE
    n      int := coalesce(array_length(p_source, 1), 0);
    rc     smallint[] := array_fill(0::smallint, ARRAY[n]);
    cur    bigint;
    i      int;
    a_src  text[]   := '{}';
    a_seq  bigint[] := '{}';
    a_eff  bytea[]  := '{}';
BEGIN
    IF n = 0 THEN RETURN rc; END IF;
    PERFORM 1 FROM octopus.source WHERE source = ANY(p_source) ORDER BY source FOR UPDATE;
    FOR i IN SELECT o FROM unnest(p_source, p_seq) WITH ORDINALITY AS u(s, q, o) ORDER BY s, q LOOP
        SELECT last_seq INTO cur FROM octopus.source WHERE source = p_source[i];
        IF NOT FOUND THEN
            rc[i] := 2;
        ELSIF cur = p_seq[i] - 1 THEN
            UPDATE octopus.source SET last_seq = p_seq[i] WHERE source = p_source[i];
            rc[i] := 0;
            a_src := a_src || p_source[i];
            a_seq := a_seq || p_seq[i];
            a_eff := a_eff || p_effect[i];
        ELSIF cur >= p_seq[i] THEN
            rc[i] := 1;
        ELSE
            rc[i] := 2;
        END IF;
    END LOOP;
    IF p_hook IS NOT NULL AND array_length(a_src, 1) > 0 THEN
        EXECUTE format('SELECT %s($1, $2, $3)', p_hook::regproc) USING a_src, a_seq, a_eff;
    END IF;
    DELETE FROM octopus.event e
     USING (SELECT source, last_seq FROM octopus.source WHERE source = ANY(p_source)) s
     WHERE e.source = s.source AND e.seq <= s.last_seq;
    RETURN rc;
END $$;

-- Remove events reported done (workers committed their own writes), and return each source's next event if it
-- is stored, so the dispatcher can hand it out without searching. rc: 0 removed now, 1 already removed, 2 gap.
CREATE OR REPLACE FUNCTION octopus.remove_next(p_source text[], p_seq bigint[])
RETURNS TABLE(i int, rc smallint, next_seq bigint, next_body bytea, next_attempts int, next_enqueued_us bigint)
LANGUAGE plpgsql AS $$
DECLARE
    codes smallint[];
BEGIN
    codes := octopus.commit_many(p_source, p_seq, array_fill(''::bytea, ARRAY[coalesce(array_length(p_source, 1), 0)]), NULL);
    RETURN QUERY
    SELECT u.o::int, codes[u.o], e.seq, e.body, e.attempts, (extract(epoch FROM e.enqueued_at) * 1000000)::bigint
      FROM unnest(p_source) WITH ORDINALITY AS u(src, o)
      JOIN octopus.source s ON s.source = u.src
      LEFT JOIN octopus.event e ON e.source = u.src AND e.seq = s.last_seq + 1
                               AND (e.not_before IS NULL OR e.not_before <= now());
END $$;

-- Rows left at or below their cursor by a publish/commit race on a source that has not committed since.
CREATE OR REPLACE FUNCTION octopus.purge_applied(p_limit int DEFAULT 10000)
RETURNS bigint LANGUAGE sql AS $$
    WITH d AS (
        DELETE FROM octopus.event e
         WHERE (e.source, e.seq) IN (
            SELECT e2.source, e2.seq FROM octopus.event e2 JOIN octopus.source s ON s.source = e2.source
             WHERE e2.seq <= s.last_seq LIMIT p_limit)
        RETURNING 1)
    SELECT count(*) FROM d
$$;
