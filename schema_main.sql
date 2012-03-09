CREATE TABLE IF NOT EXISTS client (
    -- ip addr and last time each enctype list was seen, maybe more
    -- stats
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ip TEXT NOT NULL UNIQUE, -- This could be an integer if ipv4, or whatever
                      -- is the best native type for IP addresses in the
		      -- given RDBMS.  Text will do though.
    last_success INTEGER NOT NULL DEFAULT 0,
    last_fail INTEGER NOT NULL DEFAULT 0,
    last_weak_as INTEGER NOT NULL DEFAULT 0,
    last_weak_tgs INTEGER NOT NULL DEFAULT 0,
    last_strong_as INTEGER NOT NULL DEFAULT 0,
    last_strong_tgs INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS client_slice_data (
    ip TEXT PRIMARY KEY REFERENCES client (ip) ON DELETE NO ACTION,
    starttime INTEGER NOT NULL,
    endtime INTEGER NOT NULL,
    nentries INTEGER NOT NULL DEFAULT 0,
    is_success INTEGER NOT NULL,
    nweak_as INTEGER NOT NULL DEFAULT 0,
    nweak_tgs INTEGER NOT NULL DEFAULT 0,
    nstrong_as INTEGER NOT NULL DEFAULT 0,
    nstrong_tgs INTEGER NOT NULL DEFAULT 0,
    weak_as_rate REAL,
    weak_tgs_rate REAL,
    strong_as_rate REAL,
    strong_tgs_rate REAL
);

CREATE TABLE IF NOT EXISTS princ (
    -- princ name, princ id, last auth, last fail, ...
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    host TEXT, -- non-NULL if princ is host-based
    last_used INTEGER NOT NULL DEFAULT 0, -- either as client or server
    -- for brevity lc == last_client, ls == last_server
    lc_auth INTEGER NOT NULL DEFAULT 0,
    lc_fail INTEGER NOT NULL DEFAULT 0,
    -- We could have a table with per-{princ, enctype} rows
    --
    -- These are regarding the requested enctypes
    lc_req_had_weak INTEGER NOT NULL DEFAULT 0,
    lc_req_had_weakfirst INTEGER NOT NULL DEFAULT 0,
    lc_req_had_strong INTEGER NOT NULL DEFAULT 0,
    lc_req_had_strongonly INTEGER NOT NULL DEFAULT 0,
    -- These are regarding replies
    lc_req_got_weaksesskey INTEGER NOT NULL DEFAULT 0,
    lc_req_got_weakreply INTEGER NOT NULL DEFAULT 0,
    lc_req_got_strongsesskey INTEGER NOT NULL DEFAULT 0,
    lc_req_got_strongreply INTEGER NOT NULL DEFAULT 0,
    -- Session and reply keys were strong
    lc_req_got_strong INTEGER NOT NULL DEFAULT 0,
    -- Ticket stuff
    --
    -- Last ticket issued for this princ name as a service
    ls_ticket_issue INTEGER NOT NULL DEFAULT 0,
    ls_ticket_fail INTEGER NOT NULL DEFAULT 0,
    ls_ticket_weaksesskey INTEGER NOT NULL DEFAULT 0,
    ls_ticket_weakticketkey INTEGER NOT NULL DEFAULT 0,
    ls_ticket_strongsesskey INTEGER NOT NULL DEFAULT 0,
    ls_ticket_strongticketkey INTEGER NOT NULL DEFAULT 0,
    ls_ticket_strong INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS princ_slice_data (
    name TEXT PRIMARY KEY REFERENCES princ (name) ON DELETE NO ACTION,
    starttime INTEGER NOT NULL,
    endtime INTEGER NOT NULL,
    nentries INTEGER NOT NULL,
    -- for brevity lc == last_client, ls == last_server
    nauth INTEGER NOT NULL DEFAULT 0,
    nfail INTEGER NOT NULL DEFAULT 0,
    nreq_had_weak INTEGER NOT NULL DEFAULT 0,
    nreq_had_weakfirst INTEGER NOT NULL DEFAULT 0,
    nreq_had_strong INTEGER NOT NULL DEFAULT 0,
    nreq_had_strongonly INTEGER NOT NULL DEFAULT 0,
    nreq_got_weaksesskey INTEGER NOT NULL DEFAULT 0,
    nreq_got_weakreply INTEGER NOT NULL DEFAULT 0,
    nreq_got_strongsesskey INTEGER NOT NULL DEFAULT 0,
    nreq_got_strongreply INTEGER NOT NULL DEFAULT 0,
    nreq_got_strong INTEGER NOT NULL DEFAULT 0,
    nticket_issue INTEGER NOT NULL DEFAULT 0,
    nticket_fail INTEGER NOT NULL DEFAULT 0,
    nticket_weaksesskey INTEGER NOT NULL DEFAULT 0,
    nticket_weakticketkey INTEGER NOT NULL DEFAULT 0,
    nticket_strongsesskey INTEGER NOT NULL DEFAULT 0,
    nticket_strongticketkey INTEGER NOT NULL DEFAULT 0,
    nticket_strong INTEGER NOT NULL DEFAULT 0,
    req_had_weak_rate REAL,
    req_had_weakfirst_rate REAL,
    req_had_strong_rate REAL,
    req_had_strongonly_rate REAL,
    req_got_weaksesskey_rate REAL,
    req_got_weakreply_rate REAL,
    req_got_strongsesskey_rate REAL,
    req_got_strongreply_rate REAL,
    req_got_strong_rate REAL,
    ticket_issue_rate REAL,
    ticket_fail_rate REAL,
    ticket_weaksesskey_rate REAL,
    ticket_weakticketkey_rate REAL,
    ticket_strongsesskey_rate REAL,
    ticket_strongticketkey_rate REAL,
    ticket_strong_rate REAL
);

CREATE TABLE IF NOT EXISTS client_cname_sname (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ip_id INTEGER NOT NULL REFERENCES client (id),
    cname_id INTEGER NOT NULL REFERENCES princ (id),
    sname_id INTEGER NOT NULL REFERENCES princ (id),
    last_weak_sess_key INTEGER NOT NULL DEFAULT 0,
    last_strong_sess_key INTEGER NOT NULL DEFAULT 0,
    UNIQUE(ip_id, cname_id, sname_id)
);

CREATE TABLE IF NOT EXISTS css_slice_data (
    id INTEGER PRIMARY KEY REFERENCES client_cname_sname (id) ON DELETE NO ACTION,
    starttime INTEGER NOT NULL,
    endtime INTEGER NOT NULL,
    nentries INTEGER NOT NULL,
    nlast_weak_sess_key INTEGER NOT NULL DEFAULT 0,
    nlast_strong_sess_key INTEGER NOT NULL DEFAULT 0,
    last_weak_sess_key_rate REAL,
    last_strong_sess_key_rate REAL
);

CREATE VIEW IF NOT EXISTS ccsv AS
SELECT c.ip, cn.name, sn.name, ccs.last_weak_sess_key, ccs.last_strong_sess_key
FROM client c, princ cn, princ sn, client_cname_sname ccs
WHERE c.id = ccs.ip_id AND cn.id = ccs.cname_id AND sn.id = ccs.sname_id;

CREATE TABLE IF NOT EXISTS enctypes (
    enctype INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    is_modern INTEGER NOT NULL DEFAULT 0,
    is_weak INTEGER NOT NULL DEFAULT 0,
    UNIQUE (name)
);
-- Early enctypes
INSERT OR IGNORE INTO enctypes (enctype, name, is_weak) VALUES (0, 'null', 1);
INSERT OR IGNORE INTO enctypes (enctype, name, is_weak) VALUES (1, 'des-cbc-crc', 1);
INSERT OR IGNORE INTO enctypes (enctype, name, is_weak) VALUES (2, 'des-cbc-md4', 1);
INSERT OR IGNORE INTO enctypes (enctype, name, is_weak) VALUES (3, 'des-cbc-md5', 1);
INSERT OR IGNORE INTO enctypes (enctype, name, is_weak) VALUES (4, 'des-cbc-raw', 1);
INSERT OR IGNORE INTO enctypes (enctype, name, is_weak) VALUES (5, 'des3-cbc-md5', 1);
-- This one is from Heimdal, name made up here
INSERT OR IGNORE INTO enctypes (enctype, name, is_weak) VALUES (7, 'old-des3-cbc-sha1', 1);
-- These two are from MIT
INSERT OR IGNORE INTO enctypes (enctype, name, is_weak) VALUES (6, 'des3-cbc-raw', 1);
INSERT OR IGNORE INTO enctypes (enctype, name, is_weak) VALUES (8, 'des-hmac-sha1', 1);
-- These are from PKINIT, RFC4556
INSERT OR IGNORE INTO enctypes (enctype, name) VALUES (9, 'id-dsa-with-sha1');
INSERT OR IGNORE INTO enctypes (enctype, name) VALUES (10, 'md5WithRSAEncryption');
INSERT OR IGNORE INTO enctypes (enctype, name) VALUES (11, 'sha-1WithRSAEncryption');
INSERT OR IGNORE INTO enctypes (enctype, name) VALUES (12, 'rc2-cbc');
INSERT OR IGNORE INTO enctypes (enctype, name) VALUES (13, 'rsaEncryption');
INSERT OR IGNORE INTO enctypes (enctype, name) VALUES (14, 'id-RSAES-OAEP');
INSERT OR IGNORE INTO enctypes (enctype, name) VALUES (15, 'des-ede3-cbc');
-- Modern 3DES, AES, and ARCFOUR
INSERT OR IGNORE INTO enctypes (enctype, name) VALUES (16, 'des3-cbc-sha1');
INSERT OR IGNORE INTO enctypes (enctype, name, is_modern) VALUES (17, 'aes128-cts-hmac-sha1-96', 1);
INSERT OR IGNORE INTO enctypes (enctype, name, is_modern) VALUES (18, 'aes256-cts-hmac-sha1-96', 1);
INSERT OR IGNORE INTO enctypes (enctype, name) VALUES (23, 'arcfour-hmac');
INSERT OR IGNORE INTO enctypes (enctype, name, is_weak) VALUES (24, 'arcfour-hmac-exp', 1);
-- Old and who knows what stuff (taken from Heimdal)
INSERT OR IGNORE INTO enctypes (enctype, name) VALUES (48, 'pkcross');
INSERT OR IGNORE INTO enctypes (enctype, name) VALUES (-128, 'arcfour-hmac-old');
INSERT OR IGNORE INTO enctypes (enctype, name) VALUES (-133, 'arcfour-hmac-old-exp');
INSERT OR IGNORE INTO enctypes (enctype, name, is_weak) VALUES (-4096, 'des-cbc-none', 1);
INSERT OR IGNORE INTO enctypes (enctype, name, is_weak) VALUES (-4097, 'des3-cbc-none', 1);
INSERT OR IGNORE INTO enctypes (enctype, name, is_weak) VALUES (-4098, 'des-cfb64-none', 1);
INSERT OR IGNORE INTO enctypes (enctype, name, is_weak) VALUES (-4099, 'des-pcbc-none', 1);
-- Pseudo-enctypes (taken from Heimdal, intended to be used not-on-the-wire)
INSERT OR IGNORE INTO enctypes (enctype, name) VALUES (-4100, 'digest-md5-none');
INSERT OR IGNORE INTO enctypes (enctype, name) VALUES (-4101, 'cram-md5-none');

CREATE TABLE IF NOT EXISTS req_enctypes_lists (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    enctype_list TEXT NOT NULL,
    enctype_list_orig TEXT NOT NULL,
    enctype_name_list TEXT,
    modern_client INTEGER NOT NULL DEFAULT 0,
    is_weak INTEGER NOT NULL DEFAULT 1,
    is_too_weak INTEGER NOT NULL DEFAULT 1,
    client_type TEXT,
    UNIQUE (enctype_list, enctype_list_orig)
);

CREATE TABLE IF NOT EXISTS req_enctypes_normal (
    list_id INTEGER NOT NULL REFERENCES req_enctypes_lists (id),
    enctype_num INTEGER NOT NULL,
    seq_num INTEGER,
    partial_enctype_list TEXT,
    partial_enctype_name_list TEXT,
    UNIQUE (list_id, enctype_num)
);

-- The following view is used just for its instead of insert trigger
-- which is used to drive the setting of seq_num and such things in
-- req_enctypes_normal rows.  See the enctype_list_norm trigger for
-- how this view is used.
CREATE VIEW IF NOT EXISTS junk AS SELECT 0 AS el_id, '' AS el_list;
DROP TRIGGER IF EXISTS set_seq_num;
CREATE TRIGGER IF NOT EXISTS set_seq_num INSTEAD OF INSERT ON junk
FOR EACH ROW
BEGIN
    -- Update the zeroth enctype in the list if it's not already been
    -- set
    UPDATE req_enctypes_normal
    SET seq_num = 0,
	partial_enctype_list = CAST(enctype_num AS TEXT),
	partial_enctype_name_list = (SELECT e.name FROM enctypes e WHERE e.enctype = enctype_num)
    WHERE list_id = NEW.el_id AND seq_num IS NULL AND (
	NEW.el_list LIKE enctype_num OR
	NEW.el_list LIKE enctype_num || ' %');
    -- Update the next enctype in the list; the first time this will be
    -- the one that comes after the zeroth enctype, and so on.
    UPDATE req_enctypes_normal
    SET seq_num = (
	    SELECT max(en.seq_num) + 1 FROM req_enctypes_normal en
	    WHERE en.list_id = NEW.el_id AND en.seq_num IS NOT NULL),
	partial_enctype_list = (
	    SELECT en.partial_enctype_list || ' ' FROM req_enctypes_normal en
	    WHERE en.list_id = NEW.el_id
	    ORDER BY en.seq_num DESC LIMIT 1) || enctype_num,
	partial_enctype_name_list = (
	    SELECT en.partial_enctype_name_list FROM req_enctypes_normal en
	    WHERE en.list_id = NEW.el_id AND en.partial_enctype_name_list IS NOT NULL
	    ORDER BY en.seq_num DESC LIMIT 1) ||
	',' || (
	    SELECT e.name FROM enctypes e WHERE e.enctype = enctype_num)
    WHERE list_id = NEW.el_id AND seq_num IS NULL AND (
	NEW.el_list LIKE (
	    SELECT en.partial_enctype_list FROM req_enctypes_normal en
	    WHERE en.list_id = NEW.el_id
	    ORDER BY en.seq_num DESC LIMIT 1) || ' ' || enctype_num OR
	NEW.el_list LIKE (
	    SELECT en.partial_enctype_list FROM req_enctypes_normal en
	    WHERE en.list_id = NEW.el_id
	    ORDER BY en.seq_num DESC LIMIT 1) || ' ' || enctype_num || ' %');
END;

DROP TRIGGER IF EXISTS enctype_list_norm;
CREATE TRIGGER IF NOT EXISTS enctype_list_norm AFTER INSERT ON req_enctypes_lists
FOR EACH ROW
BEGIN
    -- Split enctype list into rows in req_enctypes_normal
    --
    -- This is a very neat trick.  We use the enctypes list and the LIKE
    -- operator to insert a row for each " <enctype-number> " patter in
    -- the list.  To make this work for the first and last numbers in
    -- the list we add whitespace on the ends of it.
    INSERT OR IGNORE INTO req_enctypes_normal
    (list_id, enctype_num)
    SELECT NEW.id, e.enctype FROM enctypes e
    WHERE ' ' || NEW.enctype_list || ' ' LIKE '% ' || e.enctype || ' %';
    -- Now set seq_num and the partial lists for the normalized enctype
    -- list rows.
    --
    -- This is a very neat trick.  We insert into the "junk" view as
    -- many times as we have enctypes in the list.  The instead-of
    -- insert trigger on the junk view will do all the magic.
    INSERT INTO junk (el_id, el_list)
    SELECT NEW.id, NEW.enctype_list
    FROM req_enctypes_normal en
    WHERE en.list_id = NEW.id;
    -- Now set enctype_name_list and other things in
    -- req_enctypes_lists
    UPDATE req_enctypes_lists
    SET
	-- pretty-print enctype list
	enctype_name_list = (
	    SELECT en.partial_enctype_name_list
	    FROM req_enctypes_normal en
	    WHERE en.list_id = NEW.id
	    ORDER BY seq_num DESC LIMIT 1),
	-- Does the client do AES or ARCFOUR?
	modern_client = EXISTS (
	    SELECT e.enctype FROM req_enctypes_normal en
	    JOIN enctypes e ON en.enctype_num = e.enctype
	    WHERE en.list_id = NEW.id AND e.is_modern),
	-- Does the client offer 1DES or 40-bit ARCFOUR?
	is_weak = EXISTS (
	    SELECT e.enctype FROM req_enctypes_normal en
	    JOIN enctypes e ON en.enctype_num = e.enctype
	    WHERE en.list_id = NEW.id AND e.is_weak),
	-- Does the client offer a weak enctype before any strong ones?
	is_too_weak = (
	    SELECT coalesce(min(en.seq_num), 0) FROM req_enctypes_normal en
	    JOIN enctypes e ON en.enctype_num = e.enctype
	    WHERE en.list_id = NEW.id AND e.is_weak
	) < (
	    SELECT coalesce(min(en.seq_num), 0) FROM req_enctypes_normal en
	    JOIN enctypes e ON en.enctype_num = e.enctype
	    WHERE en.list_id = NEW.id AND NOT e.is_weak
	)
    WHERE id = NEW.id;
END;
