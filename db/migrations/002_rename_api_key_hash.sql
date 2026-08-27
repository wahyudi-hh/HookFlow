ALTER TABLE clients
RENAME COLUMN api_key_hash TO api_key_digest;

ALTER TABLE clients
RENAME CONSTRAINT clients_api_key_hash_key
TO clients_api_key_digest_key;