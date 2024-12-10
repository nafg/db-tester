CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  username VARCHAR(50) NOT NULL UNIQUE,
  email VARCHAR(255) NOT NULL UNIQUE,
  display_name VARCHAR(100),
  bio TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE posts (
  id SERIAL PRIMARY KEY,
  author_id INTEGER NOT NULL REFERENCES users(id),
  content TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  parent_post_id INTEGER REFERENCES posts(id),  -- For replies
  shared_post_id INTEGER REFERENCES posts(id),  -- For reposts
  is_edited BOOLEAN NOT NULL DEFAULT FALSE,
  CONSTRAINT valid_post_type CHECK (
    (parent_post_id IS NULL AND shared_post_id IS NULL) OR  -- Original post
    (parent_post_id IS NOT NULL AND shared_post_id IS NULL) OR  -- Reply
    (parent_post_id IS NULL AND shared_post_id IS NOT NULL)  -- Repost
  )
);

CREATE TABLE hashtags (
  id SERIAL PRIMARY KEY,
  tag VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE post_hashtags (
  post_id INTEGER REFERENCES posts(id),
  hashtag_id INTEGER REFERENCES hashtags(id),
  PRIMARY KEY (post_id, hashtag_id)
);

CREATE TABLE likes (
  user_id INTEGER REFERENCES users(id),
  post_id INTEGER REFERENCES posts(id),
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (user_id, post_id)
);

CREATE TABLE follows (
  follower_id INTEGER REFERENCES users(id),
  following_id INTEGER REFERENCES users(id),
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (follower_id, following_id),
  CHECK (follower_id != following_id)
);
