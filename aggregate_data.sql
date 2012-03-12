
-- Aggregate log data in relation to the `client` table into smaller
-- (size-wise) slices
INSERT OR REPLACE INTO client_slice_data
(ip, starttime, endtime, nentries, is_success, nweak_as, nweak_tgs, nstrong_as,
nstrong_tgs, nmodern_as, nmodern_tgs, weak_as_rate, weak_tgs_rate,
strong_as_rate, strong_tgs_rate, modern_as_rate, modern_tgs_rate)
SELECT ls.ip,
    (min(ls.log_time) / 21600) * 21600 AS starttime,
    (max(ls.log_time) / 21600) * 21600 + 21599 AS endtime,
    count(*), 1,
    sum(CASE WHEN (NOT ls.req_type AND el.is_weak) THEN 1 ELSE 0 END) +
	coalesce(csd.nweak_as, 0),
    sum(CASE WHEN (ls.req_type AND el.is_weak) THEN 1 ELSE 0 END) +
	coalesce(csd.nweak_tgs, 0),
    sum(CASE WHEN (NOT ls.req_type AND NOT el.is_weak) THEN 1 ELSE 0 END) +
	coalesce(csd.nstrong_as, 0),
    sum(CASE WHEN (ls.req_type AND NOT el.is_weak) THEN 1 ELSE 0 END) +
	coalesce(csd.nstrong_tgs, 0),
    sum(CASE WHEN (NOT ls.req_type AND NOT el.modern_client) THEN 1 ELSE 0 END) +
	coalesce(csd.nmodern_as, 0),
    sum(CASE WHEN (ls.req_type AND NOT el.modern_client) THEN 1 ELSE 0 END) +
	coalesce(csd.nmodern_tgs, 0),
    csd.weak_as_rate, csd.weak_tgs_rate, csd.strong_as_rate,
    csd.strong_tgs_rate, csd.modern_as_rate, csd.modern_tgs_rate
FROM log_entry_success ls
LEFT OUTER JOIN client_slice_data csd USING (ip)
JOIN req_enctypes_lists el ON ls.req_enctypes = el.enctype_list
WHERE ls.log_time BETWEEN csd.starttime AND csd.endtime
GROUP BY ls.ip, (ls.log_time / 21600) * 21600;
-- Compute rates
UPDATE client_slice_data
SET weak_as_rate    = coalesce(nweak_as, 0) / nentries,
    weak_tgs_rate   = coalesce(nweak_tgs, 0) / nentries,
    strong_as_rate  = coalesce(nstrong_as, 0) / nentries,
    strong_tgs_rate = coalesce(nstrong_tgs, 0) / nentries
    modern_as_rate  = coalesce(nmodern_as, 0) / nentries,
    modern_tgs_rate = coalesce(nmodern_tgs, 0) / nentries
WHERE nentries != 0 AND weak_as_rate = NULL;


-- Aggregate log data in relation to the `client` table into smaller
-- (size-wise) slices
INSERT OR REPLACE INTO princ_slice_data
(name, starttime, endtime, nentries, nauth, nfail, nreq_had_weak,
 nreq_had_weakfirst, nreq_had_strong, nreq_had_strongonly,
 nreq_got_weaksesskey, nreq_got_weakreply, nreq_got_strongsesskey,
 nreq_got_strongreply, nreq_got_strong, req_had_weak_rate,
 req_had_weakfirst_rate, req_had_strong_rate, req_had_strongonly_rate,
 req_got_weaksesskey_rate, req_got_weakreply_rate, req_got_strongsesskey_rate,
 req_got_strongreply_rate, req_got_strong_rate, ticket_issue_rate,
 ticket_fail_rate, ticket_weaksesskey_rate, ticket_weakticketkey_rate,
 ticket_strongsesskey_rate, ticket_strongticketkey_rate, ticket_strong_rate)
SELECT ls.client_name,
    min(ls.log_time) AS starttime, max(ls.log_time) AS endtime, count(*),
    -- nauth
    sum(CASE WHEN ls.req_type THEN 0 ELSE 1 END) +
	coalesce(psd.nauth, 0),
    -- nfail
    0,
    sum(CASE WHEN el.is_weak THEN 1 ELSE 0 END) +
	coalesce(psd.nreq_had_weak, 0),
    sum(CASE WHEN el.is_too_weak THEN 1 ELSE 0 END) +
	coalesce(psd.nreq_had_weakfirst, 0),
    -- nreq_had_strong
    sum(CASE WHEN el.modern_client THEN 1 ELSE 0 END) +
	coalesce(psd.nreq_had_strong, 0),
    sum(CASE WHEN NOT el.is_weak THEN 1 ELSE 0 END) +
	coalesce(psd.nreq_had_strongonly, 0),
    -- nreq_got_weaksesskey
    sum(CASE WHEN se.is_weak THEN 1 ELSE 0 END) +
	coalesce(psd.nreq_got_weaksesskey, 0),
    sum(CASE WHEN re.is_weak THEN 1 ELSE 0 END) +
	coalesce(psd.nreq_got_weakreply, 0),
    -- nreq_got_strongsesskey
    sum(CASE WHEN NOT se.is_weak THEN 1 ELSE 0 END) +
	coalesce(psd.nreq_got_strongsesskey, 0),
    sum(CASE WHEN NOT re.is_weak THEN 1 ELSE 0 END) +
	coalesce(psd.nreq_got_strongreply, 0),
    sum(CASE WHEN (NOT re.is_weak AND NOT se.is_weak) THEN 1 ELSE 0 END) +
	coalesce(psd.nreq_got_strong, 0),
    psd.req_had_weak_rate, psd.req_had_weakfirst_rate, psd.req_had_strong_rate,
    psd.req_had_strongonly_rate, psd.req_got_weaksesskey_rate,
    psd.req_got_weakreply_rate, psd.req_got_strongsesskey_rate,
    psd.req_got_strongreply_rate, psd.req_got_strong_rate,
    psd.ticket_issue_rate, psd.ticket_fail_rate, psd.ticket_weaksesskey_rate,
    psd.ticket_weakticketkey_rate, psd.ticket_strongsesskey_rate,
    psd.ticket_strongticketkey_rate, psd.ticket_strong_rate
FROM log_entry_success ls
LEFT OUTER JOIN princ_slice_data psd on ls.client_name = psd.name
JOIN req_enctypes_lists el ON ls.req_enctypes = el.enctype_list
JOIN enctypes re ON ls.reply_enctype = re.enctype
JOIN enctypes se ON ls.session_enctype = se.enctype
WHERE ls.log_time BETWEEN psd.starttime AND psd.endtime
GROUP BY ls.client_name, ls.log_time / 21600;
-- Compute rates
UPDATE princ_slice_data
SET req_had_weak_rate = coalesce(nreq_had_weak, 0) / nentries,
    req_had_weakfirst_rate = coalesce(nreq_had_weakfirst, 0) / nentries,
    req_had_strong_rate = coalesce(nreq_had_strong, 0) / nentries,
    req_had_strongonly_rate = coalesce(nreq_had_strongonly, 0) / nentries,
    req_got_weaksesskey_rate = coalesce(nreq_got_weaksesskey, 0) / nentries,
    req_got_weakreply_rate = coalesce(nreq_got_weakreply, 0) / nentries,
    req_got_strongsesskey_rate = coalesce(nreq_got_strongsesskey, 0) / nentries,
    req_got_strongreply_rate = coalesce(nreq_got_strongreply, 0) / nentries,
    req_got_strong_rate = coalesce(nreq_got_strong, 0) / nentries,
    ticket_issue_rate = coalesce(nticket_issue, 0) / nentries,
    ticket_fail_rate = coalesce(nticket_fail, 0) / nentries,
    ticket_weaksesskey_rate = coalesce(nticket_weaksesskey, 0) / nentries,
    ticket_weakticketkey_rate = coalesce(nticket_weakticketkey, 0) / nentries,
    ticket_strongsesskey_rate = coalesce(nticket_strongsesskey, 0) / nentries,
    ticket_strongticketkey_rate = coalesce(nticket_strongticketkey, 0) / nentries,
    ticket_strong_rate = coalesce(nticket_strong, 0) / nentries
WHERE nentries != 0 AND nauth = NULL;

