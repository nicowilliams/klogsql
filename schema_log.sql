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
