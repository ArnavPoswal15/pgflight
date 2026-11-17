CREATE TABLE users  (id serial PRIMARY KEY, name text NOT NULL);
CREATE TABLE orders (id serial PRIMARY KEY, user_id int NOT NULL REFERENCES users(id), created_at timestamptz DEFAULT now());
CREATE TABLE items  (id serial PRIMARY KEY, order_id int NOT NULL REFERENCES orders(id), sku text, qty int);
INSERT INTO users  SELECT g, 'user' || g FROM generate_series(1, 1000) g;
INSERT INTO orders SELECT g, (g % 1000) + 1 FROM generate_series(1, 50000) g;
INSERT INTO items  SELECT g, (g % 50000) + 1, 'sku' || (g % 500), 1 + g % 5 FROM generate_series(1, 200000) g;
ANALYZE;
