CREATE TABLE IF NOT EXISTS documents (
  doc_id SERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  body TEXT NOT NULL,
  embedding vector(3) NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_documents_demo_title ON documents (title);

INSERT INTO documents (title, body, embedding) VALUES
  ('Postgres', 'En relationsdatabase med mange udvidelser.', '[0.9, 0.1, 0.1]'),
  ('Søgning', 'Fuzzy og fulltext er gode til produktkataloger.', '[0.2, 0.9, 0.1]'),
  ('Geodata', 'PostGIS kan afstande, indekser og ruteanalyse.', '[0.1, 0.2, 0.9]')
ON CONFLICT (title) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_documents_embedding ON documents USING ivfflat (embedding vector_l2_ops) WITH (lists = 10);

-- “Semantisk” søgning: find nærmeste vektorer (demo med 3D embeddings for overskuelighed)
SELECT title, body, embedding <-> '[0.85, 0.1, 0.1]' AS distance
FROM documents
ORDER BY embedding <-> '[0.85, 0.1, 0.1]'
LIMIT 3;

