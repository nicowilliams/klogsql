
-- Aggregate log data in relation to the `client` table into smaller
-- (size-wise) slices
INSERT OR REPLACE INTO client_slice_data
(ip, starttime, endtime, nentries, is_success, nweak_as, nweak_tgs, nstrong_as, nstrong_tgs)
SELECT ls.ip, min(ls.log_time), max(ls.log_time), count(ls.log_time), 1,
    (SELECT count(ls2.log_time)
	FROM log_entry_success ls2
	JOIN req_enctypes_lists el ON ls2.req_enctypes = el.enctype_list
	WHERE ls2.log_time BETWEEN
	    ls.log_time % 21600 AND ls.log_time % 21600 + 21599 AND
	NOT ls2.req_type AND el.is_weak),
    (SELECT count(ls2.log_time)
	FROM log_entry_success ls2
	JOIN req_enctypes_lists el ON ls2.req_enctypes = el.enctype_list
	WHERE ls2.log_time BETWEEN
	    ls.log_time % 21600 AND ls.log_time % 21600 + 21599 AND
	ls2.req_type AND el.is_weak),
    (SELECT count(ls2.log_time)
	FROM log_entry_success ls2
	JOIN req_enctypes_lists el ON ls2.req_enctypes = el.enctype_list
	WHERE ls2.log_time BETWEEN
	    ls.log_time % 21600 AND ls.log_time % 21600 + 21599 AND
	NOT ls2.req_type AND NOT el.is_weak),
    (SELECT count(ls2.log_time)
	FROM log_entry_success ls2
	JOIN req_enctypes_lists el ON ls2.req_enctypes = el.enctype_list
	WHERE ls2.log_time BETWEEN
	    ls.log_time % 21600 AND ls.log_time % 21600 + 21599 AND
	ls2.req_type AND NOT el.is_weak)
FROM log_entry_success ls GROUP BY ls.log_time % 21600;
-- Compute rates
UPDATE client_slice_data
SET weak_as_rate = nweak_as / nentries,
    weak_tgs_rate = nweak_tgs / nentries,
    strong_as_rate = nstrong_as / nentries,
    strong_tgs_rate = nstrong_tgs / nentries
WHERE nentries != 0 AND weak_as_rate = NULL;


-- Aggregate log data in relation to the `client` table into smaller
-- (size-wise) slices
INSERT OR REPLACE INTO princ_slice_data
(name, starttime, endtime, nentries, nauth, nfail, nreq_had_weak,
 nreq_had_weakfirst, nreq_had_strong, nreq_had_strongonly,
 nreq_got_weaksesskey, nreq_got_weakreply, nreq_got_strongsesskey,
 nreq_got_strongreply, nreq_got_strong)
SELECT ls.client_name, min(ls.log_time), max(ls.log_time), count(ls.log_time),
    -- nauth
    (SELECT count(ls2.log_time)
	FROM log_entry_success ls2
	WHERE ls2.log_time BETWEEN
	    ls.log_time % 21600 AND ls.log_time % 21600 + 21599 AND
	NOT ls2.req_type),
    -- nfail
    0,
    (SELECT count(ls2.log_time)
	FROM log_entry_success ls2
	JOIN req_enctypes_lists el ON ls2.req_enctypes = el.enctype_list
	WHERE ls2.log_time BETWEEN
	    ls.log_time % 21600 AND ls.log_time % 21600 + 21599 AND
	el.is_weak),
    (SELECT count(ls2.log_time)
	FROM log_entry_success ls2
	JOIN req_enctypes_lists el ON ls2.req_enctypes = el.enctype_list
	WHERE ls2.log_time BETWEEN
	    ls.log_time % 21600 AND ls.log_time % 21600 + 21599 AND
	el.is_too_weak),
    -- nreq_had_strong
    (SELECT count(ls2.log_time)
	FROM log_entry_success ls2
	JOIN req_enctypes_lists el ON ls2.req_enctypes = el.enctype_list
	WHERE ls2.log_time BETWEEN
	    ls.log_time % 21600 AND ls.log_time % 21600 + 21599 AND
	el.modern_client),
    (SELECT count(ls2.log_time)
	FROM log_entry_success ls2
	JOIN req_enctypes_lists el ON ls2.req_enctypes = el.enctype_list
	WHERE ls2.log_time BETWEEN
	    ls.log_time % 21600 AND ls.log_time % 21600 + 21599 AND
	NOT el.is_weak),
    -- nreq_got_weaksesskey
    (SELECT count(ls2.log_time)
	FROM log_entry_success ls2
	JOIN enctypes e ON ls2.session_enctype = e.enctype
	WHERE ls2.log_time BETWEEN
	    ls.log_time % 21600 AND ls.log_time % 21600 + 21599 AND
	e.is_weak),
    (SELECT count(ls2.log_time)
	FROM log_entry_success ls2
	JOIN enctypes e ON ls2.reply_enctype = e.enctype
	WHERE ls2.log_time BETWEEN
	    ls.log_time % 21600 AND ls.log_time % 21600 + 21599 AND
	e.is_weak),
    -- nreq_got_strongsesskey
    (SELECT count(ls2.log_time)
	FROM log_entry_success ls2
	JOIN enctypes e ON ls2.session_enctype = e.enctype
	WHERE ls2.log_time BETWEEN
	    ls.log_time % 21600 AND ls.log_time % 21600 + 21599 AND
	NOT e.is_weak),
    (SELECT count(ls2.log_time)
	FROM log_entry_success ls2
	JOIN enctypes e ON ls2.reply_enctype = e.enctype
	WHERE ls2.log_time BETWEEN
	    ls.log_time % 21600 AND ls.log_time % 21600 + 21599 AND
	NOT e.is_weak),
    (SELECT count(ls2.log_time)
	FROM log_entry_success ls2
	JOIN enctypes e ON ls2.session_enctype = e.enctype
	JOIN enctypes e2 ON ls2.reply_enctype = e2.enctype
	WHERE ls2.log_time BETWEEN
	    ls.log_time % 21600 AND ls.log_time % 21600 + 21599 AND
	NOT e.is_weak AND NOT e2.is_weak)
FROM log_entry_success ls GROUP BY ls.log_time % 21600;
-- Compute rates
UPDATE client_slice_data
SET weak_as_rate = nweak_as / nentries,
    weak_tgs_rate = nweak_tgs / nentries,
    strong_as_rate = nstrong_as / nentries,
    strong_tgs_rate = nstrong_tgs / nentries
WHERE nentries != 0 AND weak_as_rate = NULL;

