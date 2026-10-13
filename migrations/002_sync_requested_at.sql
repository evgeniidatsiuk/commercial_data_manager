-- When the oldest unsynced change was requested; drives the cdm.sync.lag SLI.
ALTER TABLE sync_state ADD COLUMN requested_at timestamptz NOT NULL DEFAULT now();
