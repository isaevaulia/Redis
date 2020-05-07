import mysql.connector
import redis
from mysql.connector import Error
import create_base as db

r = redis.Redis(host='redis', port=6379, db=0)
connection = mysql.connector.connect(host='base', user='root', password='root', port = 3306)

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as e:
        print(f"The error '{e}' occurred")

create_database_query = "CREATE DATABASE sm_app"
create_database(connection, create_database_query)


connection = mysql.connector.connect(host='base', user='root', database='sm_app', password='root', port = 3306)

drop_likes_table = """ DROP TABLE likes;"""
drop_comments_table = """ DROP TABLE comments;"""
drop_posts_table = """ DROP TABLE posts ;"""
drop_users_table = """ DROP TABLE users ;"""

db.execute_query(connection, drop_likes_table)
db.execute_query(connection, drop_comments_table)
db.execute_query(connection, drop_posts_table)
db.execute_query(connection, drop_users_table)

create_users_table = """
CREATE TABLE IF NOT EXISTS users (
  id INT AUTO_INCREMENT,
  name TEXT NOT NULL,
  age INT,
  gender TEXT,
  nationality TEXT,
  PRIMARY KEY (id)
) ENGINE = InnoDB AUTO_INCREMENT=1;
"""

create_posts_table = """
CREATE TABLE IF NOT EXISTS posts (
  id INT AUTO_INCREMENT,
  title TEXT NOT NULL,
  description TEXT NOT NULL,
  user_id INTEGER NOT NULL,
  FOREIGN KEY fk_user_id (user_id) REFERENCES users(id),
  PRIMARY KEY (id)
) ENGINE = InnoDB AUTO_INCREMENT=1;
"""

create_likes_table = """
CREATE TABLE IF NOT EXISTS comments (
  id INT AUTO_INCREMENT,
  text TEXT NOT NULL,
  user_id INT NOT NULL,
  post_id INT NOT NULL,
  FOREIGN KEY fk_user_id (user_id) REFERENCES users (id),
  FOREIGN KEY fk_post_id (post_id) REFERENCES posts (id),
  PRIMARY KEY (id)
)
ENGINE = InnoDB AUTO_INCREMENT=1;
"""

create_comments_table = """
CREATE TABLE IF NOT EXISTS likes (
  id INT AUTO_INCREMENT,
  user_id INT NOT NULL,
  post_id INT NOT NULL,
  FOREIGN KEY fk_user_id (user_id) REFERENCES users (id),
  FOREIGN KEY fk_post_id (post_id) REFERENCES posts (id),
  PRIMARY KEY (id)
)ENGINE = InnoDB AUTO_INCREMENT=1;
"""


db.execute_query(connection, create_users_table)
db.execute_query(connection, create_posts_table)
db.execute_query(connection, create_likes_table)
db.execute_query(connection, create_comments_table)

create_users = """
INSERT INTO
  users (name, age, gender, nationality)
VALUES
  ('James', 25, 'male', 'USA'),
  ('Leila', 32, 'female', 'France'),
  ('Brigitte', 35, 'female', 'England'),
  ('Mike', 40, 'male', 'Denmark'),
  ('Elizabeth', 21, 'female', 'Canada');
"""
create_posts = """
INSERT INTO
  posts (title, description, user_id)
VALUES
  ("Happy", "I am feeling very happy today", 1),
  ("Hot Weather", "The weather is very hot today", 2),
  ("Help", "I need some help with my work", 2),
  ("Great News", "I am getting married", 1),
  ("Interesting Game", "It was a fantastic game of tennis", 5),
  ("Party", "Anyone up for a late-night party today?", 3);
"""
create_comments = """
INSERT INTO
  comments (text, user_id, post_id)
VALUES
  ('Count me in', 1, 6),
  ('What sort of help?', 5, 3),
  ('Congrats buddy', 2, 4),
  ('I was rooting for Nadal though', 4, 5),
  ('Help with your thesis?', 2, 3),
  ('Many congratulations', 5, 4);

"""

create_likes = """
INSERT INTO
  likes (user_id, post_id)
VALUES
  (1, 6),
  (2, 3),
  (1, 5),
  (5, 4),
  (2, 4),
  (4, 2),
  (3, 6);
"""


db.execute_query(connection, create_users)
db.execute_query(connection, create_posts)
db.execute_query(connection, create_comments)
db.execute_query(connection, create_likes)

#выберем все записи из таблицы users
select_users = "SELECT * from users"

#ледующий скрипт возвращает идентификаторы и имена пользователей, а также описание сообщений, опубликованных этими пользователями:
select_users_posts = """
SELECT
  users.id,
  users.name,
  posts.description
FROM
  posts
  INNER JOIN users ON users.id = posts.user_id
"""

#SELECT-запрос, который возвращает текст поста и общее количество лайков, им полученных:
select_post_likes = """
SELECT
  description as Post,
  COUNT(likes.id) as Likes
FROM
  likes,
  posts
WHERE
  posts.id = likes.post_id
GROUP BY
  likes.post_id
"""


print("\n\n\n\n")
print("Hello this is bd Users, Posts, Comments, Likes\n")

while (True):
    db.work(r,connection)