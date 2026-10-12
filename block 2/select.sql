SELECT 
    (SELECT COUNT(*) FROM books) AS total_books,
    (SELECT COUNT(*) FROM students) AS total_students;

SELECT id, name, birth_date FROM students 
ORDER BY birth_date;

SELECT id, title, author, year FROM books 
WHERE year = (SELECT MAX(year) FROM books);

SELECT id, title, author, year FROM books 
ORDER BY year DESC LIMIT 1;