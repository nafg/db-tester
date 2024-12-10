CREATE TABLE departments (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100) NOT NULL UNIQUE,
  building VARCHAR(100)
);

CREATE TABLE professors (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  department_id INTEGER REFERENCES departments(id),
  email VARCHAR(255) NOT NULL UNIQUE,
  hire_date DATE NOT NULL
);

CREATE TABLE courses (
  id SERIAL PRIMARY KEY,
  code VARCHAR(20) NOT NULL UNIQUE,
  name VARCHAR(255) NOT NULL,
  department_id INTEGER REFERENCES departments(id),
  professor_id INTEGER REFERENCES professors(id),
  credits INTEGER NOT NULL CHECK (credits > 0),
  max_students INTEGER CHECK (max_students > 0)
);

CREATE TABLE students (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255) NOT NULL UNIQUE,
  enrollment_date DATE NOT NULL,
  major_department_id INTEGER REFERENCES departments(id),
  advisor_id INTEGER REFERENCES professors(id)
);

CREATE TABLE enrollments (
  id SERIAL PRIMARY KEY,
  student_id INTEGER REFERENCES students(id),
  course_id INTEGER REFERENCES courses(id),
  semester VARCHAR(20) NOT NULL,
  year INTEGER NOT NULL CHECK (year > 1900),
  grade VARCHAR(2) CHECK (grade IN ('A+', 'A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-', 'D+', 'D', 'F', 'W', 'I')),
  UNIQUE(student_id, course_id, semester, year)
);
