/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
SELECT category_id, COUNT(film_id)
FROM film_category
GROUP BY category_id
ORDER BY category_id DESC;


/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
WITH top_most_rented_movies AS (
    SELECT
        film_id,
        COUNT(film_id) as num_rented
    FROM inventory
    GROUP BY film_id
    ORDER BY num_rented DESC
    LIMIT 10
)
SELECT
    CONCAT(actor.first_name, ' ', actor.last_name) AS most_popular_actors
FROM actor
INNER JOIN film_actor ON actor.actor_id = film_actor.actor_id
INNER JOIN top_most_rented_movies ON top_most_rented_movies.film_id = film_actor.film_id
ORDER BY actor.first_name
LIMIT 10;

/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
WITH category_revenue (category_id, total_revenue) AS (
    SELECT
        film_category.category_id,
        SUM(p.amount) AS total_revenue
    FROM film_category
    JOIN inventory ON film_category.film_id = inventory.film_id
    JOIN rental r ON inventory.inventory_id = r.inventory_id
    JOIN payment p ON r.rental_id = p.rental_id
    GROUP BY film_category.category_id
),
category_names AS (
    SELECT
        category.category_id,
        category.name AS category_name
    FROM category
)
SELECT
    category_names.category_name,
    COALESCE(category_revenue.total_revenue, 0) AS revenue
FROM category_names
LEFT JOIN category_revenue ON category_names.category_id = category_revenue.category_id
ORDER BY total_revenue DESC
LIMIT 1;


/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
SELECT
    film.film_id,
    film.title
FROM film
WHERE NOT EXISTS (
    SELECT DISTINCT inventory.film_id
    FROM inventory
    WHERE film.film_id = inventory.film_id
);



/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
SELECT
    actor.actor_id,
    actor.first_name,
    actor.last_name,
    COUNT(DISTINCT film_id) AS num_films_played
FROM actor
INNER JOIN film_actor ON actor.actor_id = film_actor.actor_id
WHERE actor.actor_id IN (
    -- select movies in Children's category
    SELECT film_category.film_id AS film_id
    FROM film
    INNER JOIN film_category on film.film_id = film_category.film_id
    WHERE film_category.category_id = 3
)
GROUP BY actor.actor_id
ORDER BY num_films_played DESC
LIMIT 3;