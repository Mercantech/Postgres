CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    tags TEXT[],
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Trigram-indekser (fuzzy matching)
CREATE INDEX IF NOT EXISTS idx_products_name_trgm ON products USING GIN (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_products_description_trgm ON products USING GIN (description gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_products_tags ON products USING GIN (tags);

INSERT INTO products (name, description, tags)
SELECT v.n, v.d, v.t
FROM (
  VALUES
    ('iPhone 14 Pro', 'Apples flagskib smartphone med det bedste kamera nogensinde',
     ARRAY['elektronik', 'mobil', 'apple']::text[]),
    ('Samsung Galaxy S23', 'Premium Android-telefon med fantastisk skærm fra Samsung',
     ARRAY['elektronik', 'mobil', 'samsung']::text[]),
    ('MacBook Air M2', 'Let og kraftfuld laptop med lang batterilevetid fra Apple',
     ARRAY['elektronik', 'computer', 'apple']::text[]),
    ('Bose QuietComfort 45', 'Premium støjreducerende hovedtelefoner fra Bose',
     ARRAY['elektronik', 'lyd', 'hovedtelefoner']::text[])
) AS v(n, d, t)
WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.name = v.n);

-- Fuzzy søgning med similarity() (pg_trgm)
CREATE OR REPLACE FUNCTION search_products(
    search_term TEXT,
    similarity_threshold DOUBLE PRECISION DEFAULT 0.3
) RETURNS TABLE (
    product_id INTEGER,
    name VARCHAR(100),
    description TEXT,
    tags TEXT[],
    similarity DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        p.product_id,
        p.name,
        p.description,
        p.tags,
        GREATEST(
            similarity(p.name, search_term)::double precision,
            similarity(p.description, search_term)::double precision
        ) AS similarity
    FROM products p
    WHERE
        similarity(p.name, search_term)::double precision > similarity_threshold
        OR similarity(p.description, search_term)::double precision > similarity_threshold
        OR EXISTS (
            SELECT 1
            FROM unnest(p.tags) tag
            WHERE similarity(tag, search_term)::double precision > similarity_threshold
        )
    ORDER BY similarity DESC;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM search_products('iphone', 0.3);
SELECT * FROM search_products('telefon', 0.25);

-- Full text søgning (dansk)
CREATE MATERIALIZED VIEW IF NOT EXISTS product_search_index AS
SELECT
    product_id,
    to_tsvector(
      'danish',
      unaccent(name) || ' ' || unaccent(coalesce(description, '')) || ' ' || unaccent(array_to_string(tags, ' '))
    ) AS document
FROM products;

CREATE INDEX IF NOT EXISTS idx_product_search ON product_search_index USING GIN (document);

-- Opdatér søgeindeks efter evt. nye rækker (sikkert ved genkørsel af demo)
REFRESH MATERIALIZED VIEW product_search_index;

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
        ts_rank(psi.document, websearch_to_tsquery('danish', unaccent(search_query))) AS rank
    FROM product_search_index psi
    JOIN products p ON p.product_id = psi.product_id
    WHERE psi.document @@ websearch_to_tsquery('danish', unaccent(search_query))
    ORDER BY rank DESC;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fulltext_search_products('apple mobil');

