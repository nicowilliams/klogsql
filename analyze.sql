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
    -- Pay attention to the use of max() as an aggregate and not aggregate
    max(coalesce(c.last_success, 0), max(ls.log_time)),
    c.last_fail,
    max(coalesce(c.last_weak_as, 0), max(CASE WHEN NOT ls.req_type AND el.is_weak THEN ls.log_time ELSE 0 END)),
    max(coalesce(c.last_weak_tgs, 0), max(CASE WHEN ls.req_type AND el.is_weak THEN ls.log_time ELSE 0 END)),
    max(coalesce(c.last_strong_as, 0), max(CASE WHEN NOT ls.req_type AND NOT el.is_weak THEN ls.log_time ELSE 0 END)),
    max(coalesce(c.last_strong_tgs, 0), max(CASE WHEN ls.req_type AND NOT el.is_weak THEN ls.log_time ELSE 0 END))
FROM log_entry_success ls
JOIN req_enctypes_lists el ON ls.req_enctypes = el.enctype_list
LEFT OUTER JOIN client c ON ls.ip = c.ip
GROUP BY ls.ip;

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
    max(coalesce(p.lc_auth, 0), max(ls.log_time), coalesce(p.ls_ticket_issue, 0)),
    max(coalesce(p.lc_auth, 0), max(ls.authtime)),
    p.lc_fail,
    max(coalesce(p.lc_req_had_weak, 0), max(CASE WHEN el.is_weak THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_had_weakfirst, 0), max(CASE WHEN el.is_too_weak THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_had_strong, 0), max(CASE WHEN NOT el.modern_client THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_had_strongonly, 0), max(CASE WHEN NOT el.is_weak THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_got_weaksesskey, 0), max(CASE WHEN se.is_weak THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_got_weakreply, 0), max(CASE WHEN re.is_weak THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_got_strongsesskey, 0), max(CASE WHEN NOT se.is_weak THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_got_strongreply, 0), max(CASE WHEN NOT re.is_weak THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_got_strong, 0), max(CASE WHEN NOT se.is_weak AND NOT se.is_weak THEN ls.log_time ELSE 0 END)),
    -- Leave these alone; we set them in a JOIN on ls.server_name
    p.ls_ticket_issue, p.ls_ticket_fail, p.ls_ticket_weaksesskey,
    p.ls_ticket_weakticketkey, p.ls_ticket_strongsesskey,
    p.ls_ticket_strongticketkey, p.ls_ticket_strong
FROM log_entry_success ls
LEFT OUTER JOIN princ p ON ls.client_name = p.name
JOIN enctypes re ON ls.reply_enctype = re.enctype
JOIN enctypes se ON ls.session_enctype = re.enctype
JOIN req_enctypes_lists el ON ls.req_enctypes = el.enctype_list
GROUP BY ls.client_name;

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
    max(coalesce(p.lc_auth, 0), max(ls.log_time), coalesce(p.ls_ticket_issue, 0)),
    max(coalesce(p.lc_auth, 0), max(ls.authtime)),
    p.lc_fail,
    -- Leave these alone; we set them in a JOIN on ls.client_name
    p.lc_req_had_weak, p.lc_req_had_weakfirst, p.lc_req_had_strong,
    p.lc_req_had_strongonly, p.lc_req_got_weaksesskey, p.lc_req_got_weakreply,
    p.lc_req_got_strongsesskey, p.lc_req_got_strongreply, p.lc_req_got_strong,
    max(coalesce(p.ls_ticket_issue, 0), max(ls.log_time)),
    p.ls_ticket_fail,
    max(coalesce(p.ls_ticket_weaksesskey, 0), max(CASE WHEN se.is_weak THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.ls_ticket_weakticketkey, 0), max(CASE WHEN te.is_weak THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.ls_ticket_strongsesskey, 0), max(CASE WHEN NOT se.is_weak THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.ls_ticket_strongticketkey, 0), max(CASE WHEN NOT te.is_weak THEN ls.log_time ELSE 0 END)),
    max(coalesce(p.ls_ticket_strong, 0), max(CASE WHEN NOT se.is_weak AND NOT te.is_weak THEN ls.log_time ELSE 0 END))
FROM log_entry_success ls
LEFT OUTER JOIN princ p ON ls.server_name = p.name
JOIN enctypes se ON ls.session_enctype = se.enctype
JOIN enctypes te ON ls.ticket_enctype = te.enctype
GROUP BY ls.server_name;


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
    max(coalesce(p.lc_req_had_weak, 0), max(CASE WHEN el.is_weak THEN lf.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_had_weakfirst, 0), max(CASE WHEN el.is_too_weak THEN lf.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_had_strong, 0), max(CASE WHEN el.modern_client THEN lf.log_time ELSE 0 END)),
    max(coalesce(p.lc_req_had_strongonly, 0), max(CASE WHEN NOT el.is_weak THEN lf.log_time ELSE 0 END)),
    -- This being a failure, we can't update these
    p.lc_req_got_weaksesskey,
    p.lc_req_got_weakreply,
    p.lc_req_got_strongsesskey,
    p.lc_req_got_strongreply,
    p.lc_req_got_strong, 
    p.ls_ticket_issue, p.ls_ticket_fail, p.ls_ticket_weaksesskey,
    p.ls_ticket_weakticketkey, p.ls_ticket_strongsesskey,
    p.ls_ticket_strongticketkey, p.ls_ticket_strong
FROM log_entry_fail lf
LEFT OUTER JOIN princ p ON lf.client_name = p.name
JOIN req_enctypes_lists el ON lf.req_enctypes = el.enctype_list
GROUP BY lf.client_name;

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
    max(lf.log_time),
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
FROM log_entry_fail lf LEFT OUTER JOIN princ p ON lf.client_name = p.name
GROUP BY lf.client_name;;

--
INSERT OR REPLACE INTO client_cname_sname
(ip, cname, sname, last_weak_sess_key, last_strong_sess_key)
SELECT 
    ls.ip, ls.client_name, ls.server_name,
    max(coalesce(ccs.last_weak_sess_key, 0), max(CASE WHEN e.is_weak THEN ls.log_time ELSE 0 END)),
    max(coalesce(ccs.last_strong_sess_key, 0), max(CASE WHEN NOT e.is_weak THEN ls.log_time ELSE 0 END))
       -- XXX Add more analysis!
FROM log_entry_success ls
INNER JOIN enctypes e ON ls.session_enctype = e.enctype
LEFT OUTER JOIN client_cname_sname ccs ON ls.ip = ccs.ip AND ls.client_name = ccs.cname AND ls.server_name = ccs.sname
GROUP BY ls.ip, ls.client_name, ls.server_name;
