import mysql.connector
from mysql.connector import Error

def create_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as e:
        print(f"The error '{e}' occurred")

def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
    except Error as e:
        print("The error '{e}' occurred")
    return "successful querty"


drop_likes_table = """ DROP TABLE likes;"""
drop_comments_table = """ DROP TABLE comments;"""
drop_posts_table = """ DROP TABLE posts ;"""
drop_users_table = """ DROP TABLE users ;"""

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

create_comments_table = """
CREATE TABLE IF NOT EXISTS comments (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  text TEXT NOT NULL,
  user_id INTEGER NOT NULL,
  post_id INTEGER NOT NULL,
  FOREIGN KEY fk_user_id (user_id) REFERENCES users (id),
  FOREIGN KEY fk_post_id (post_id) REFERENCES posts (id)
)
ENGINE = InnoDB AUTO_INCREMENT=1;
"""

create_likes_table = """
CREATE TABLE IF NOT EXISTS likes (
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  user_id INTEGER NOT NULL,
  post_id integer NOT NULL,
  FOREIGN KEY fk_user_id (user_id) REFERENCES users (id),
  FOREIGN KEY fk_post_id (post_id) REFERENCES posts (id)
)ENGINE = InnoDB AUTO_INCREMENT=1;
"""

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

def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")


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

def do_querty(connection, querty):
    a = execute_read_query(connection, querty)
    stroka = ''
    for i in a:
        stroka += str(i)
    return stroka

def print_querty(r, connection, querty,key):
    if r.exists(key):
        print(r.get(key).decode("utf-8"))
        print("From Redis")
    else:
        a = execute_read_query(connection, querty)
        stroka = ''
        for i in a:
            stroka += str(i)
        print(stroka)
    return "successful print"



def work(r, connection):

    print("Chose querty (1,2,3)\n")
    print("1: All users\n")
    print("2: Users with id and their posts\n")
    print("3: Posts \n")

    querty_int=input()
    if (querty_int == "1"):
        querty = select_users
        key = "Users"
    elif (querty_int == "2"):
        querty = select_users_posts
        key = "Posts"
    elif (querty_int == "3"):
        querty = select_post_likes
        key = "Likes"
    else:
        print("No such querty")
        return "error querty"

    print("Do you want save querty to Redis? (yes/no)\n")
    save = input()

    if (save == "no"):
        print_querty(r, connection, querty,key)
    elif (save == "yes"):
        print("Set TTL (in seconds)")
        sec = int(input())
        r.setex(key, sec, do_querty(connection, querty))
        print_querty(r, connection, querty, key)
        print("Succesыful save to Redis ")
    else:
        return "error save"

    print("Do you want try again? (yes/no)\n")
    end = input()
    if (end == "yes"):
        return "Successful querty"
    elif (end == "no"):
        input("Press Enter to continue...")
    else:
        return "New querty"

    return "Successful querty"




