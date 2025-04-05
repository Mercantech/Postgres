-- @block Først sikrer vi at pgcrypto extension er installeret
CREATE EXTENSION IF NOT EXISTS pgcrypto;


-- @block Opret users tabel med krypterede passwords
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    -- Krypteret data kolonne til følsomme oplysninger
    sensitive_data TEXT
);

-- @block Funktion til at oprette en ny bruger med krypteret password
CREATE OR REPLACE FUNCTION create_user(
    p_username VARCHAR,
    p_password VARCHAR,
    p_email VARCHAR,
    p_sensitive_data TEXT
) RETURNS INTEGER AS $$
DECLARE
    new_user_id INTEGER;
BEGIN
    INSERT INTO users (
        username,
        password_hash,
        email,
        sensitive_data
    ) VALUES (
        p_username,
        crypt(p_password, gen_salt('bf')), -- Bruger Blowfish (bf) til password hashing
        p_email,
        pgp_sym_encrypt(p_sensitive_data, 'MinHemmeligeNøgle') -- Symmetrisk kryptering af følsomme data
    ) RETURNING user_id INTO new_user_id;
    
    RETURN new_user_id;
END;
$$ LANGUAGE plpgsql;

-- @block Funktion til at verificere login
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

-- @block Test data og eksempler på brug
-- Opret nogle test brugere
SELECT create_user('alice', 'password123', 'alice@example.com', 'CPR: 123456-7890');
SELECT create_user('bob', 'securepass456', 'bob@example.com', 'CPR: 098765-4321');

-- @block Test login verifikation
SELECT verify_user('alice', 'password123') as login_success; -- Skulle returnere true
SELECT verify_user('alice', 'wrongPassword') as login_failure; -- Skulle returnere false

-- @block Vis brugere (bemærk at passwords er hashet og sensitive_data er krypteret)
SELECT user_id, username, email, 
       password_hash,
       pgp_sym_decrypt(sensitive_data::bytea, 'MinHemmeligeNøgle') as decrypted_sensitive_data
FROM users;

-- @block Eksempel på søgning efter bruger med bestemt email (case insensitive)
CREATE INDEX idx_users_email_lower ON users (LOWER(email));

-- @block Funktion til at opdatere password
CREATE OR REPLACE FUNCTION update_user_password(
    p_username VARCHAR,
    p_old_password VARCHAR,
    p_new_password VARCHAR
) RETURNS BOOLEAN AS $$
BEGIN
    IF NOT verify_user(p_username, p_old_password) THEN
        RETURN FALSE;
    END IF;
    
    UPDATE users 
    SET password_hash = crypt(p_new_password, gen_salt('bf'))
    WHERE username = p_username;
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;
