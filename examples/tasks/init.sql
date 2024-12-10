CREATE TABLE projects (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  description TEXT,
  start_date DATE NOT NULL,
  end_date DATE,
  CHECK (end_date IS NULL OR end_date >= start_date)
);

CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255) NOT NULL UNIQUE,
  role VARCHAR(50) NOT NULL CHECK (role IN ('manager', 'developer', 'tester'))
);

CREATE TABLE tasks (
  id SERIAL PRIMARY KEY,
  project_id INTEGER REFERENCES projects(id),
  title VARCHAR(255) NOT NULL,
  description TEXT,
  status VARCHAR(50) NOT NULL CHECK (status IN ('todo', 'in_progress', 'blocked', 'completed')),
  assigned_to INTEGER REFERENCES users(id),
  created_by INTEGER REFERENCES users(id),
  depends_on INTEGER REFERENCES tasks(id),  -- Task this one depends on
  blocked_by INTEGER REFERENCES tasks(id),  -- Task actively blocking this one
  priority INTEGER CHECK (priority BETWEEN 1 AND 5),
  estimated_hours INTEGER,
  due_date DATE,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  completed_at TIMESTAMP,
  CHECK (depends_on != id),  -- Can't depend on itself
  CHECK (blocked_by != id),  -- Can't be blocked by itself
  CHECK (blocked_by != depends_on),  -- Can't be both blocking and dependent
  CHECK (completed_at IS NULL OR completed_at >= created_at)
);
