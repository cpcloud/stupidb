DROP TABLE IF EXISTS t;
CREATE TABLE t (
  z TEXT,
  a INTEGER,
  b INTEGER,
  e INTEGER
);
INSERT INTO t VALUES
  ('a', 1, 2, 1),
  ('b', 2, -1, 2),
  ('a', 3, 4, 3),
  ('a', 4, -3, 4),
  ('a', 1, -3, 5),
  ('b', 2, -3, 6),
  ('b', 3, -3, 7);
DROP TABLE IF EXISTS s;
CREATE TABLE s (
  z TEXT,
  a INTEGER,
  b INTEGER,
  e INTEGER
);
INSERT INTO s VALUES
  ('a', 1, 2, 1),
  ('c', 2, -1, 2),
  ('a', 3, 4, 3),
  ('c', 4, -3, 4),
  ('a', 1, -3, 5),
  ('c', 2, -3, 6),
  ('c', 3, -3, 7);
