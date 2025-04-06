-- @block Først installerer vi de nødvendige extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- For trigram matching
CREATE EXTENSION IF NOT EXISTS unaccent; -- For at håndtere accenter i tekst
CREATE EXTENSION IF NOT EXISTS pg_search;


-- @block Lav en products tabel som eksempel
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    tags TEXT[],
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- @block Opret GiST og GIN indekser for effektiv søgning
CREATE INDEX idx_products_name_trgm ON products USING GIN (name gin_trgm_ops);
CREATE INDEX idx_products_description_trgm ON products USING GIN (description gin_trgm_ops);
CREATE INDEX idx_products_tags ON products USING GIN (tags);

-- @block Indsæt nogle test produkter
INSERT INTO products (name, description, tags) VALUES
    ('iPhone 14 Pro', 'Apples flagskib smartphone med det bedste kamera nogensinde', 
     ARRAY['elektronik', 'mobil', 'apple']),
    ('Samsung Galaxy S23', 'Premium Android-telefon med fantastisk skærm fra Samsung', 
     ARRAY['elektronik', 'mobil', 'samsung']),
    ('MacBook Air M2', 'Let og kraftfuld laptop med lang batterilevetid fra Apple', 
     ARRAY['elektronik', 'computer', 'apple']),
    ('Bose QuietComfort 45', 'Premium støjreducerende hovedtelefoner fra Bose', 
     ARRAY['elektronik', 'lyd', 'hovedtelefoner']);

-- @block Vis alle produkter
SELECT * FROM products;

-- @block Funktion til fuzzy søgning i produkter
CREATE OR REPLACE FUNCTION search_products(
    search_term TEXT,
    similarity_threshold FLOAT DEFAULT 0.3
) RETURNS TABLE (
    product_id INTEGER,
    name VARCHAR(100),
    description TEXT,
    tags TEXT[],
    similarity FLOAT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        p.product_id,
        p.name,
        p.description,
        p.tags,
        GREATEST(
            similarity(p.name, search_term),
            similarity(p.description, search_term)
        ) as similarity
    FROM products p
    WHERE 
        similarity(p.name, search_term) > similarity_threshold
        OR similarity(p.description, search_term) > similarity_threshold
        OR EXISTS (
            SELECT 1 
            FROM unnest(p.tags) tag 
            WHERE similarity(tag, search_term) > similarity_threshold
        )
    ORDER BY similarity DESC;
END;
$$ LANGUAGE plpgsql;

-- @block Vis om pg_trgm er installeret
SELECT * FROM search_products('iphone', 0.3);

-- @block Test søgefunktionen med forskellige eksempler
SELECT * FROM search_products('iphone');
SELECT * FROM search_products('telefon');
SELECT * FROM search_products('apple');

-- Funktion til at søge med vægtning af forskellige felter
CREATE OR REPLACE FUNCTION weighted_search_products(
    search_term TEXT,
    name_weight FLOAT DEFAULT 1.0,
    description_weight FLOAT DEFAULT 0.5,
    tags_weight FLOAT DEFAULT 0.3
) RETURNS TABLE (
    product_id INTEGER,
    name VARCHAR(100),
    description TEXT,
    tags TEXT[],
    search_rank FLOAT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        p.product_id,
        p.name,
        p.description,
        p.tags,
        (
            similarity(p.name, search_term) * name_weight +
            similarity(p.description, search_term) * description_weight +
            COALESCE(
                (SELECT MAX(similarity(tag, search_term)) 
                FROM unnest(p.tags) tag) * tags_weight,
                0
            )
        ) as search_rank
    FROM products p
    WHERE 
        similarity(p.name, search_term) > 0.1
        OR similarity(p.description, search_term) > 0.1
        OR EXISTS (
            SELECT 1 
            FROM unnest(p.tags) tag 
            WHERE similarity(tag, search_term) > 0.1
        )
    ORDER BY search_rank DESC;
END;
$$ LANGUAGE plpgsql;

-- @block Test den vægtede søgning
SELECT * FROM weighted_search_products('mobil');
SELECT * FROM weighted_search_products('apple produkter');

-- First, create the materialized view if it doesn't exist
CREATE MATERIALIZED VIEW IF NOT EXISTS product_search_index AS
SELECT 
    product_id,
    name,
    description,
    tags,
    to_tsvector('danish', name || ' ' || description || ' ' || array_to_string(tags, ' ')) as document
FROM products;

-- Create the index on the materialized view
CREATE INDEX IF NOT EXISTS idx_product_search ON product_search_index USING GIN (document);

-- Then recreate the function
CREATE OR REPLACE FUNCTION fulltext_search_products(
    search_query TEXT
) RETURNS TABLE (
    product_id INTEGER,
    name VARCHAR(100),
    description TEXT,
    tags TEXT[],
    rank FLOAT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        p.product_id,
        p.name,
        p.description,
        p.tags,
        ts_rank(psi.document, to_tsquery('danish', search_query)) as rank
    FROM product_search_index psi
    JOIN products p ON p.product_id = psi.product_id
    WHERE psi.document @@ to_tsquery('danish', search_query)
    ORDER BY rank DESC;
END;
$$ LANGUAGE plpgsql;

-- @block Test full text søgning
SELECT * FROM fulltext_search_products('apple & mobil');
