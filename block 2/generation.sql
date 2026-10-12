INSERT INTO students (name, email, birth_date, created_at)
SELECT 
    'Student_' || i AS name,
    'student_' || i || '@example.com' AS email,
    NOW()::DATE - INTERVAL '18 years' - (RANDOM() * 3650 || ' days')::INTERVAL AS birth_date,
    NOW() - (RANDOM() * 30 || ' days')::INTERVAL AS created_at
FROM generate_series(1, 50) AS i;

INSERT INTO books (title, author, year)
SELECT 
    'Book Title ' || i AS title,
    'Author ' || (1 + FLOOR(RANDOM() * 50)) AS author,
    FLOOR(1950 + RANDOM() * (2026 - 1950 + 1))::INT AS year
FROM generate_series(1, 50) AS i;