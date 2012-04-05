
-- Copyright (c) 2012, Secure Endpoints Inc.
-- All rights reserved.
--
-- Redistribution and use in source and binary forms, with or without
-- modification, are permitted provided that the following conditions
-- are met:
--
-- - Redistributions of source code must retain the above copyright
--   notice, this list of conditions and the following disclaimer.
--
-- - Redistributions in binary form must reproduce the above copyright
--   notice, this list of conditions and the following disclaimer in
--   the documentation and/or other materials provided with the
--   distribution.
--
-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
-- "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
-- LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
-- FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
-- COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
-- INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
-- (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
-- SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
-- HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
-- STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
-- ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
-- OF THE POSSIBILITY OF SUCH DAMAGE.


-- Aggregate log data in relation to the `client` table into smaller
-- (size-wise) slices
--
-- We use a temp table to speed things up
CREATE TEMP TABLE IF NOT EXISTS tcsd AS SELECT * FROM client_slice_data LIMIT 0;
DELETE FROM tcsd;
INSERT OR REPLACE INTO tcsd
(ip, starttime, endtime, nentries, is_success, nweak_as, nweak_tgs, nstrong_as,
nstrong_tgs, nmodern_as, nmodern_tgs)
SELECT ls.ip,
    (min(ls.log_time) / 86400) * 86400,
    (max(ls.log_time) / 86400) * 86400 + 86399,
    count(*), 1,
    sum(CASE WHEN (NOT ls.req_type AND el.is_weak) THEN 1 ELSE 0 END),
    sum(CASE WHEN (ls.req_type AND el.is_weak) THEN 1 ELSE 0 END),
    sum(CASE WHEN (NOT ls.req_type AND NOT el.is_weak) THEN 1 ELSE 0 END),
    sum(CASE WHEN (ls.req_type AND NOT el.is_weak) THEN 1 ELSE 0 END),
    sum(CASE WHEN (NOT ls.req_type AND NOT el.modern_client) THEN 1 ELSE 0 END),
    sum(CASE WHEN (ls.req_type AND NOT el.modern_client) THEN 1 ELSE 0 END)
FROM log_entry_success ls
JOIN req_enctypes_lists el ON ls.req_enctypes = el.enctype_list
GROUP BY ls.ip, (ls.log_time / 86400) * 86400;

CREATE INDEX IF NOT EXISTS tcsdi ON tcsd(ip, starttime);
--
-- Merge the temp table into the main one and compute averages
INSERT OR REPLACE INTO client_slice_data
(ip, starttime, endtime, nentries, is_success, nweak_as, nweak_tgs, nstrong_as,
nstrong_tgs, nmodern_as, nmodern_tgs, weak_as_rate, weak_tgs_rate,
strong_as_rate, strong_tgs_rate, modern_as_rate, modern_tgs_rate)
SELECT tcsd.ip,
    tcsd.starttime,
    tcsd.endtime,
    tcsd.nentries + coalesce(csd.nentries, 0),
    1,
    tcsd.nweak_as + coalesce(csd.nweak_as, 0),
    tcsd.nweak_tgs + coalesce(csd.nweak_tgs, 0),
    tcsd.nstrong_as + coalesce(csd.nstrong_as, 0),
    tcsd.nstrong_tgs + coalesce(csd.nstrong_tgs, 0),
    tcsd.nmodern_as + coalesce(csd.nmodern_as, 0),
    tcsd.nmodern_tgs + coalesce(csd.nmodern_tgs, 0),
    tcsd.nweak_as + coalesce(csd.nweak_as, 0) / tcsd.nentries + coalesce(csd.nentries, 0),
    tcsd.nweak_tgs + coalesce(csd.nweak_as, 0) / tcsd.nentries + coalesce(csd.nentries, 0),
    tcsd.nstrong_as + coalesce(csd.nweak_as, 0) / tcsd.nentries + coalesce(csd.nentries, 0),
    tcsd.nstrong_tgs + coalesce(csd.nweak_as, 0) / tcsd.nentries + coalesce(csd.nentries, 0),
    tcsd.nmodern_as + coalesce(csd.nmodern_as, 0) / tcsd.nentries + coalesce(csd.nentries, 0),
    tcsd.nmodern_tgs + coalesce(csd.nmodern_tgs, 0) / tcsd.nentries + coalesce(csd.nentries, 0)
FROM tcsd tcsd
LEFT OUTER JOIN client_slice_data csd USING (ip, starttime, is_success);

-- Aggregate log data in relation to the `client` table into smaller
-- (size-wise) slices
CREATE TEMP TABLE IF NOT EXISTS tpsd AS SELECT * FROM princ_slice_data LIMIT 0;
DELETE FROM tpsd;
INSERT OR REPLACE INTO tpsd
(name, starttime, endtime, nentries, nauth, nfail, nreq_had_weak,
 nreq_had_weakfirst, nreq_had_strong, nreq_had_strongonly,
 nreq_got_weaksesskey, nreq_got_weakreply, nreq_got_strongsesskey,
 nreq_got_strongreply, nreq_got_strong)
SELECT ls.client_name,
    min(ls.log_time) AS starttime, max(ls.log_time) AS endtime, count(*),
    -- nauth
    sum(CASE WHEN ls.req_type THEN 0 ELSE 1 END),
    -- nfail
    0,
    sum(CASE WHEN el.is_weak THEN 1 ELSE 0 END),
    sum(CASE WHEN el.is_too_weak THEN 1 ELSE 0 END),
    -- nreq_had_strong
    sum(CASE WHEN el.modern_client THEN 1 ELSE 0 END),
    sum(CASE WHEN NOT el.is_weak THEN 1 ELSE 0 END),
    -- nreq_got_weaksesskey
    sum(CASE WHEN se.is_weak THEN 1 ELSE 0 END),
    sum(CASE WHEN re.is_weak THEN 1 ELSE 0 END),
    -- nreq_got_strongsesskey
    sum(CASE WHEN NOT se.is_weak THEN 1 ELSE 0 END),
    sum(CASE WHEN NOT re.is_weak THEN 1 ELSE 0 END),
    sum(CASE WHEN (NOT re.is_weak AND NOT se.is_weak) THEN 1 ELSE 0 END)
FROM log_entry_success ls
JOIN req_enctypes_lists el ON ls.req_enctypes = el.enctype_list
JOIN enctypes re ON ls.reply_enctype = re.enctype
JOIN enctypes se ON ls.session_enctype = se.enctype
GROUP BY ls.client_name, (ls.log_time / 86400) * 86400;

CREATE INDEX IF NOT EXISTS tpsdi ON tpsd(name, starttime);

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
SELECT tpsd.name,
    tpsd.starttime, tpsd.endtime, tpsd.nentries + coalesce(psd.nentries, 0),
    -- nauth
    tpsd.nauth + coalesce(psd.nauth, 0),
    -- nfail
    0,
    tpsd.nreq_had_weak + coalesce(psd.nreq_had_weak, 0),
    tpsd.nreq_had_weakfirst + coalesce(psd.nreq_had_weakfirst, 0),
    -- nreq_had_strong
    tpsd.nreq_had_strong + coalesce(psd.nreq_had_strong, 0),
    tpsd.nreq_had_strongonly + coalesce(psd.nreq_had_strongonly, 0),
    -- nreq_got_weaksesskey
    tpsd.nreq_got_weaksesskey + coalesce(psd.nreq_got_weaksesskey, 0),
    tpsd.nreq_got_weakreply + coalesce(psd.nreq_got_weakreply, 0),
    -- nreq_got_strongsesskey
    tpsd.nreq_got_strongsesskey + coalesce(psd.nreq_got_strongsesskey, 0),
    tpsd.nreq_got_strongreply + coalesce(psd.nreq_got_strongreply, 0),
    tpsd.nreq_got_strong + coalesce(psd.nreq_got_strong, 0),
    tpsd.nreq_had_weak + coalesce(psd.nreq_had_weak, 0) / tpsd.nentries + coalesce(psd.nentries, 0),
    tpsd.nreq_had_weakfirst + coalesce(psd.nreq_had_weakfirst, 0) / tpsd.nentries + coalesce(psd.nentries, 0),
    tpsd.req_had_strong_rate + coalesce(psd.req_had_strong_rate, 0) / tpsd.nentries + coalesce(psd.nentries, 0),
    tpsd.req_had_strongonly_rate + coalesce(psd.req_had_strongonly_rate, 0) / tpsd.nentries + coalesce(psd.nentries, 0),
    tpsd.req_got_weaksesskey_rate + coalesce(psd.req_got_weaksesskey_rate, 0) / tpsd.nentries + coalesce(psd.nentries, 0),
    tpsd.req_got_weakreply_rate + coalesce(psd.req_got_weakreply_rate, 0) / tpsd.nentries + coalesce(psd.nentries, 0),
    tpsd.req_got_strongsesskey_rate + coalesce(psd.req_got_strongsesskey_rate, 0) / tpsd.nentries + coalesce(psd.nentries, 0),
    tpsd.req_got_strongreply_rate + coalesce(psd.req_got_strongreply_rate, 0) / tpsd.nentries + coalesce(psd.nentries, 0),
    tpsd.req_got_strong_rate + coalesce(psd.req_got_strong_rate, 0) / tpsd.nentries + coalesce(psd.nentries, 0),
    tpsd.ticket_issue_rate + coalesce(psd.ticket_issue_rate, 0) / tpsd.nentries + coalesce(psd.nentries, 0),
    tpsd.ticket_fail_rate + coalesce(psd.ticket_fail_rate, 0) / tpsd.nentries + coalesce(psd.nentries, 0),
    tpsd.ticket_weaksesskey_rate + coalesce(psd.ticket_weaksesskey_rate, 0) / tpsd.nentries + coalesce(psd.nentries, 0),
    tpsd.ticket_weakticketkey_rate + coalesce(psd.ticket_weakticketkey_rate, 0) / tpsd.nentries + coalesce(psd.nentries, 0),
    tpsd.ticket_strongsesskey_rate + coalesce(psd.ticket_strongsesskey_rate, 0) / tpsd.nentries + coalesce(psd.nentries, 0),
    tpsd.ticket_strongticketkey_rate + coalesce(psd.ticket_strongticketkey_rate, 0) / tpsd.nentries + coalesce(psd.nentries, 0),
    tpsd.ticket_strong_rate + coalesce(psd.ticket_strong_rate, 0) / tpsd.nentries + coalesce(psd.nentries, 0)
FROM tpsd tpsd
LEFT OUTER JOIN princ_slice_data psd USING (name, starttime);

-- Heurisitcally detect services to which clients forward credentials
--INSERT OR REPLACE INTO fwdtgts
--SELECT ls.ip, ls.authtime, ls.client_name, ls.server_name, ls2.server_name,
--	ls2.log_time - ls.log_time
--FROM log_entry_success ls
--JOIN log_entry_success ls2 USING (ip, authtime, client_name)
--WHERE ls.req_type AND ls2.req_type AND
--    (ls2.log_time - ls.log_time) BETWEEN 0 AND 900 AND
--    ls2.server_name LIKE 'krbtgt%' AND ls2.req_enctypes != ls.req_enctypes AND
--    ls2.server_name != ls.server_name;

