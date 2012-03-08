--CREATE TEMP TRIGGER update_stats_success
--AFTER INSERT ON logs0.log_entry_success
--FOR EACH ROW
--BEGIN
-- Insert client, princ, and {client, cname, sname} rows so we can
-- keep stats there
INSERT OR IGNORE INTO client (ip)
SELECT ls.ip
FROM log_entry_success ls
WHERE NOT EXISTS (SELECT c.ip FROM client c WHERE c.ip = ls.ip);
--
INSERT OR IGNORE INTO princ (name)
SELECT ls.client_name
FROM log_entry_success ls
WHERE NOT EXISTS (SELECT p.name FROM princ p WHERE p.name = ls.client_name);
--
INSERT OR IGNORE INTO princ (name)
SELECT ls.server_name
FROM log_entry_success ls
WHERE NOT EXISTS (SELECT p.name FROM princ p WHERE p.name = ls.server_name);
--
INSERT OR IGNORE INTO client_cname_sname (ip_id, cname_id, sname_id)
SELECT DISTINCT (
    SELECT c.id FROM client c WHERE c.ip = ls.ip), (
    SELECT p.id FROM princ p WHERE p.name = ls.client_name), (
    SELECT p.id FROM princ p WHERE p.name = ls.server_name)
FROM log_entry_success ls;

-- Update stats
--
-- Use INSERT OR REPLACE INTO ... SELECT ... <left outer join>; to make up for
-- SQLite3's lack of support for JOINs in UPDATE statements.
--
-- No, this idiom is not perfect.  INSERT OR REPLACE is treated as
-- INSERT OR DELETE THEN INSERT.  That's a problem, but here not a big
-- deal.  Also, it's rather verbose.

-- First update the client table
INSERT OR REPLACE INTO client
(id, ip, last_success, last_fail, last_weak_as, last_weak_tgs, last_strong_as,
 last_strong_tgs)
SELECT c.id, -- Because this is a left outer join the can be null, in which case
             -- sqlite3 will auto-allocate a new rowid for us
    ls.ip,
    max(coalesce(c.last_success, 0), ls.log_time),
    c.last_fail,
    max(coalesce(c.last_weak_as, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT en.enctype_num
               FROM req_enctypes_normal en
               JOIN enctypes e ON en.enctype_num = e.enctype
               WHERE en.list_id = ls.req_enctypes AND e.is_weak) AND
                   NOT ls.req_type THEN ls.log_time ELSE 0 END)),
    max(coalesce(c.last_weak_as, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT en.enctype_num
               FROM req_enctypes_normal en
               JOIN enctypes e ON en.enctype_num = e.enctype
               WHERE en.list_id = ls.req_enctypes AND e.is_weak) AND
                   ls.req_type THEN ls.log_time ELSE 0 END)),
    max(coalesce(c.last_strong_as, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT en.enctype_num
               FROM req_enctypes_normal en
               JOIN enctypes e ON en.enctype_num = e.enctype
               WHERE en.list_id = ls.req_enctypes AND NOT e.is_weak) AND
                   NOT ls.req_type THEN ls.log_time ELSE 0 END)),
    max(coalesce(c.last_strong_tgs, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT en.enctype_num
               FROM req_enctypes_normal en
               JOIN enctypes e ON en.enctype_num = e.enctype
               WHERE en.list_id = ls.req_enctypes AND NOT e.is_weak) AND
                   ls.req_type THEN ls.log_time ELSE 0 END))
FROM log_entry_success ls LEFT OUTER JOIN client c ON ls.ip = c.ip;

-- Now update the princ table row for the client name in each success log entry
INSERT OR REPLACE INTO princ
(id, name, host, last_used, lc_auth, lc_fail, lc_req_had_weak,
 lc_req_had_weakfirst, lc_req_had_strong, lc_req_had_strongonly,
 lc_req_got_weaksesskey, lc_req_got_weakreply, lc_req_got_strongsesskey,
 lc_req_got_strongreply, lc_req_got_strong, ls_ticket_issue, ls_ticket_fail,
 ls_ticket_weaksesskey, ls_ticket_weakticketkey, ls_ticket_strongsesskey,
 ls_ticket_strongticketkey, ls_ticket_strong)
SELECT p.id, -- remember, NULL here is ok
    ls.client_name,
    p.host, -- XXX set p.host, eh?
    max(coalesce(p.lc_auth, 0), ls.log_time, coalesce(p.ls_ticket_issue, 0)),
    max(coalesce(p.lc_auth, 0), ls.authtime),
    p.lc_fail,
    max(coalesce(p.lc_req_had_weak, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT el.id
               FROM req_enctypes_lists el
               WHERE el.id = ls.req_enctypes AND el.is_weak) THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_had_weakfirst, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT el.id
               FROM req_enctypes_lists el
               WHERE el.id = ls.req_enctypes AND el.is_too_weak) THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_had_strong, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT el.id
               FROM req_enctypes_lists el
               WHERE el.id = ls.req_enctypes AND NOT el.modern_client) THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_had_strongonly, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT el.id
               FROM req_enctypes_lists el
               WHERE el.id = ls.req_enctypes AND NOT el.is_weak) THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_got_weaksesskey, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT e.enctype
               FROM enctypes e
               WHERE e.enctype = ls.session_enctype AND e.is_weak) THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_got_weakreply, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT e.enctype
               FROM enctypes e
               WHERE e.enctype = ls.reply_enctype AND e.is_weak) THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_got_strongsesskey, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT e.enctype
               FROM enctypes e
               WHERE e.enctype = ls.session_enctype AND NOT e.is_weak) THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_got_strongreply, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT e.enctype
               FROM enctypes e
               WHERE e.enctype = ls.reply_enctype AND NOT e.is_weak) THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_got_strong, 0), (
            SELECT CASE WHEN EXISTS (
                SELECT e.enctype
                FROM enctypes e, enctypes e2
                WHERE e.enctype = ls.reply_enctype AND NOT e.is_weak AND
                    e2.enctype = ls.session_enctype AND NOT e2.is_weak) THEN ls.log_time ELSE 0 END)),
    -- Leave these alone; we set them in a JOIN on ls.server_name
    p.ls_ticket_issue, p.ls_ticket_fail, p.ls_ticket_weaksesskey,
    p.ls_ticket_weakticketkey, p.ls_ticket_strongsesskey,
    p.ls_ticket_strongticketkey, p.ls_ticket_strong
FROM log_entry_success ls LEFT OUTER JOIN princ p ON ls.client_name = p.name;

-- Now update the princ table row for the server name in each success log entry
INSERT OR REPLACE INTO princ
(id, name, host, last_used, lc_auth, lc_fail, lc_req_had_weak,
 lc_req_had_weakfirst, lc_req_had_strong, lc_req_had_strongonly,
 lc_req_got_weaksesskey, lc_req_got_weakreply, lc_req_got_strongsesskey,
 lc_req_got_strongreply, lc_req_got_strong, ls_ticket_issue, ls_ticket_fail,
 ls_ticket_weaksesskey, ls_ticket_weakticketkey, ls_ticket_strongsesskey,
 ls_ticket_strongticketkey, ls_ticket_strong)
SELECT p.id, -- remember, NULL here is ok
    ls.server_name,
    p.host, -- XXX set p.host, eh?
    max(coalesce(p.lc_auth, 0), ls.log_time, coalesce(p.ls_ticket_issue, 0)),
    max(coalesce(p.lc_auth, 0), ls.authtime),
    p.lc_fail,
    -- Leave these alone; we set them in a JOIN on ls.client_name
    lc_req_had_weak, lc_req_had_weakfirst, lc_req_had_strong,
    lc_req_had_strongonly, lc_req_got_weaksesskey, lc_req_got_weakreply,
    lc_req_got_strongsesskey, lc_req_got_strongreply, lc_req_got_strong,
    max(coalesce(p.ls_ticket_issue, 0), ls.log_time),
    p.ls_ticket_fail,
    max(coalesce(p.ls_ticket_weaksesskey, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT e.enctype
               FROM enctypes e
               WHERE e.enctype = ls.session_enctype AND e.is_weak) THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.ls_ticket_weakticketkey, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT e.enctype
               FROM enctypes e
               WHERE e.enctype = ls.ticket_enctype AND e.is_weak) THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.ls_ticket_strongsesskey, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT e.enctype
               FROM enctypes e
               WHERE e.enctype = ls.session_enctype AND NOT e.is_weak) THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.ls_ticket_strongticketkey, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT e.enctype
               FROM enctypes e
               WHERE e.enctype = ls.ticket_enctype AND NOT e.is_weak) THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.ls_ticket_strong, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT e.enctype
               FROM enctypes e, enctypes e2
               WHERE e.enctype = ls.ticket_enctype AND NOT e.is_weak AND
                   e2.enctype = ls.session_enctype AND NOT e2.is_weak) THEN ls.log_time ELSE 0 END))
FROM log_entry_success ls LEFT OUTER JOIN princ p ON ls.server_name = p.name;
--
INSERT OR REPLACE INTO client_cname_sname
(id, ip_id, cname_id, sname_id, last_weak_sess_key, last_strong_sess_key)
SELECT ccs.id, -- remember, NULL here is ok
    c.id, cn.id, sn.id,
    max(coalesce(ccs.last_weak_sess_key, 0),
       CASE WHEN e.is_weak THEN ls.log_time ELSE 0 END),
    max(coalesce(ccs.last_strong_sess_key, 0),
       CASE WHEN NOT e.is_weak THEN ls.log_time ELSE 0 END)
       -- XXX Add more analysis!
FROM log_entry_success ls
INNER JOIN client c ON ls.ip = c.ip
INNER JOIN princ cn ON ls.client_name = cn.name
INNER JOIN princ sn ON ls.server_name = sn.name
INNER JOIN enctypes e ON ls.session_enctype = e.enctype
LEFT OUTER JOIN client_cname_sname ccs
WHERE ccs.ip_id = c.id AND ccs.cname_id = cn.id AND ccs.sname_id = sn.id;


-- Now update the princ table row for the client name in each fail log entry
INSERT OR REPLACE INTO princ
(id, name, host, last_used, lc_auth, lc_fail, lc_req_had_weak,
 lc_req_had_weakfirst, lc_req_had_strong, lc_req_had_strongonly,
 lc_req_got_weaksesskey, lc_req_got_weakreply, lc_req_got_strongsesskey,
 lc_req_got_strongreply, lc_req_got_strong, ls_ticket_issue, ls_ticket_fail,
 ls_ticket_weaksesskey, ls_ticket_weakticketkey, ls_ticket_strongsesskey,
 ls_ticket_strongticketkey, ls_ticket_strong)
SELECT p.id, -- remember, NULL here is ok
    lf.client_name,
    p.host, -- XXX set p.host, eh?
    p.lc_auth,
    p.lc_auth,
    lf.log_time,
    max(coalesce(p.lc_req_had_weak, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT el.id
               FROM req_enctypes_lists el
               WHERE el.id = lf.req_enctypes AND el.is_weak) THEN lf.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_had_weakfirst, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT el.id
               FROM req_enctypes_lists el
               WHERE el.id = lf.req_enctypes AND el.is_too_weak) THEN lf.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_had_strong, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT el.id
               FROM req_enctypes_lists el
               WHERE el.id = lf.req_enctypes AND NOT el.modern_client) THEN lf.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_had_strongonly, 0), (
           SELECT CASE WHEN EXISTS (
               SELECT el.id
               FROM req_enctypes_lists el
               WHERE el.id = lf.req_enctypes AND NOT el.is_weak) THEN lf.log_time ELSE 0 END)),
    -- This being a failure, we can't update these
    p.lc_req_got_weaksesskey,
    p.lc_req_got_weakreply,
    p.lc_req_got_strongsesskey,
    p.lc_req_got_strongreply,
    p.lc_req_got_strong, 
    p.ls_ticket_issue, p.ls_ticket_fail, p.ls_ticket_weaksesskey,
    p.ls_ticket_weakticketkey, p.ls_ticket_strongsesskey,
    p.ls_ticket_strongticketkey, p.ls_ticket_strong
FROM log_entry_fail lf LEFT OUTER JOIN princ p ON lf.client_name = p.name;

-- Now update the princ table row for the server name in each fail log entry
INSERT OR REPLACE INTO princ
(id, name, host, last_used, lc_auth, lc_fail, lc_req_had_weak,
 lc_req_had_weakfirst, lc_req_had_strong, lc_req_had_strongonly,
 lc_req_got_weaksesskey, lc_req_got_weakreply, lc_req_got_strongsesskey,
 lc_req_got_strongreply, lc_req_got_strong, ls_ticket_issue, ls_ticket_fail,
 ls_ticket_weaksesskey, ls_ticket_weakticketkey, ls_ticket_strongsesskey,
 ls_ticket_strongticketkey, ls_ticket_strong)
SELECT p.id, -- remember, NULL here is ok
    lf.client_name,
    p.host, -- XXX set p.host, eh?
    p.lc_auth,
    p.lc_auth,
    lf.log_time,
    p.lc_req_had_weak,
    p.lc_req_had_weakfirst,
    p.lc_req_had_strong,
    p.lc_req_had_strongonly,
    p.lc_req_got_weaksesskey,
    p.lc_req_got_weakreply,
    p.lc_req_got_strongsesskey,
    p.lc_req_got_strongreply,
    p.lc_req_got_strong, 
    p.ls_ticket_issue, p.ls_ticket_fail, p.ls_ticket_weaksesskey,
    p.ls_ticket_weakticketkey, p.ls_ticket_strongsesskey,
    p.ls_ticket_strongticketkey, p.ls_ticket_strong
FROM log_entry_fail lf LEFT OUTER JOIN princ p ON lf.client_name = p.name;

--
INSERT OR REPLACE INTO client_cname_sname
(id, ip_id, cname_id, sname_id, last_weak_sess_key, last_strong_sess_key)
SELECT ccs.id, -- remember, NULL here is ok
    c.id, cn.id, sn.id,
    max(coalesce(ccs.last_weak_sess_key, 0),
       CASE WHEN e.is_weak THEN ls.log_time ELSE 0 END),
    max(coalesce(ccs.last_strong_sess_key, 0),
       CASE WHEN NOT e.is_weak THEN ls.log_time ELSE 0 END)
       -- XXX Add more analysis!
FROM log_entry_success ls
INNER JOIN client c ON ls.ip = c.ip
INNER JOIN princ cn ON ls.client_name = cn.name
INNER JOIN princ sn ON ls.server_name = sn.name
INNER JOIN enctypes e ON ls.session_enctype = e.enctype
LEFT OUTER JOIN client_cname_sname ccs
WHERE ccs.ip_id = c.id AND ccs.cname_id = cn.id AND ccs.sname_id = sn.id;

