
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

PRAGMA page_size = 8192;

CREATE TABLE IF NOT EXISTS log_entry_success (
    -- normalized log entry
    log_time INTEGER NOT NULL,
    kdc TEXT NOT NULL,
    ip TEXT NOT NULL,
    req_type INTEGER NOT NULL CHECK (req_type = 0 OR req_type = 1), -- 0 -> AS,
    req_enctypes TEXT NOT NULL, -- REFERENCES request_enctypes_lists (enctype_list),
    req_enctypes_orig TEXT NOT NULL,
    authtime INTEGER NOT NULL,
    client_name TEXT NOT NULL,
    server_name TEXT NOT NULL,
    reply_enctype INTEGER NOT NULL,
    ticket_enctype INTEGER NOT NULL,
    session_enctype INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS log_entry_fail (
    -- normalized log entry
    log_time INTEGER NOT NULL,
    kdc TEXT NOT NULL,
    ip TEXT NOT NULL,
    req_type INTEGER CHECK (req_type = 0 OR req_type = 1), -- 0 -> AS, 1 -> TGS
    code TEXT NOT NULL,
    req_enctypes TEXT NOT NULL, -- REFERENCES request_enctypes_lists (enctype_list),
    req_enctypes_orig TEXT NOT NULL,
    client_name TEXT,
    server_name TEXT,
    reason TEXT NOT NULL
);
