CREATE TABLE authors (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  birth_date DATE,
  nationality VARCHAR(100)
);

CREATE TABLE genres (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE books (
  id SERIAL PRIMARY KEY,
  title VARCHAR(255) NOT NULL,
  author_id INTEGER REFERENCES authors(id),
  genre_id INTEGER REFERENCES genres(id),
  publication_date DATE NOT NULL,
  isbn VARCHAR(13) UNIQUE,
  price DECIMAL(10,2) NOT NULL
);
