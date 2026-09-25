-- Reverte 0001_init.up.sql. Apaga todos os dados: usar só em desenvolvimento/teste.
DROP TABLE IF EXISTS inbox_messages;
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS wallet_ledger_entries;
DROP TABLE IF EXISTS wager_transactions;
DROP TABLE IF EXISTS wallets;

DROP FUNCTION IF EXISTS outbox_guard();
DROP FUNCTION IF EXISTS wager_transactions_guard();
DROP FUNCTION IF EXISTS wallets_enforce_ledger_consistency();
DROP FUNCTION IF EXISTS wallets_guard_update();
DROP FUNCTION IF EXISTS ledger_validate_entry();
DROP FUNCTION IF EXISTS ledger_append_only();
