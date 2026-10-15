SELECT 
    (SELECT COUNT(*) FROM books) AS total_books,
    (SELECT COUNT(*) FROM students) AS total_students;
-- 50	50

SELECT id, name, birth_date FROM students 
ORDER BY birth_date;
-- 13	Student_13	1999-02-20
-- 5	Student_5	1999-04-15
-- 35	Student_35	1999-06-22
-- 32	Student_32	1999-08-10
-- 42	Student_42	1999-08-18
-- 4	Student_4	1999-08-28
-- 6	Student_6	1999-10-04
-- 47	Student_47	2000-05-26
-- 50	Student_50	2000-07-21
-- 34	Student_34	2000-08-12

SELECT id, title, author, year FROM books 
WHERE year = (SELECT MAX(year) FROM books);
-- 29	Book Title 29	Author 27	2026

SELECT id, title, author, year FROM books 
ORDER BY year DESC LIMIT 1;
-- 29	Book Title 29	Author 27	2026