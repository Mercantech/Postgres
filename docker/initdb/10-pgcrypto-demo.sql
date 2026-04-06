CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    sensitive_data BYTEA
);

CREATE OR REPLACE FUNCTION create_user(
    p_username VARCHAR,
    p_password VARCHAR,
    p_email VARCHAR,
    p_sensitive_data TEXT
) RETURNS INTEGER AS $$
DECLARE
    new_user_id INTEGER;
    k TEXT := 'klasse-demo-nøgle-skift-mig';
BEGIN
    INSERT INTO users (username, password_hash, email, sensitive_data)
    VALUES (
        p_username,
        crypt(p_password, gen_salt('bf')),
        p_email,
        pgp_sym_encrypt(p_sensitive_data, k)
    )
    RETURNING user_id INTO new_user_id;

    RETURN new_user_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION verify_user(
    p_username VARCHAR,
    p_password VARCHAR
) RETURNS BOOLEAN AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1
        FROM users
        WHERE username = p_username
          AND password_hash = crypt(p_password, password_hash)
    );
END;
$$ LANGUAGE plpgsql;

-- Demo-data (kun første init)
SELECT create_user('alice', 'password123', 'alice@example.com', 'CPR: 123456-7890');
SELECT create_user('bob', 'securepass456', 'bob@example.com', 'CPR: 098765-4321');

SELECT verify_user('alice', 'password123') AS login_success;
SELECT verify_user('alice', 'wrongPassword') AS login_failure;

-- Dekryptering (samme nøgle som ved kryptering)
WITH k AS (SELECT 'klasse-demo-nøgle-skift-mig'::text AS key)
SELECT
  user_id,
  username,
  email,
  password_hash,
  pgp_sym_decrypt(sensitive_data, (SELECT key FROM k)) AS decrypted_sensitive_data
FROM users
ORDER BY user_id;

