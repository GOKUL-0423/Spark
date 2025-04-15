# Databricks notebook source
# MAGIC %run ./Remove_hive_tables

# COMMAND ----------

"""
--A social media app tracks which users like which pages (hashtag#likes table).
--A friendship network is stored in the hashtag#friends table.
--The goal? Find pages liked by a user’s friend that the user hasn’t liked yet!
--Think of it as “Your friend liked this page, maybe you’d like it too!” 🎯

CREATE TABLE friends (
 user_id INT,
 friend_id INT
);
INSERT INTO friends VALUES
(1, 2),(1, 3),
(1, 4),(2, 1),
(3, 1),(3, 4),
(4, 1),(4, 3);

CREATE TABLE likes (
 user_id INT,
 page_id CHAR(1)
);
INSERT INTO likes VALUES
(1, 'A'),(1, 'B'),
(1, 'C'),(2, 'A'),
(3, 'B'),(3, 'C'),
(4, 'B');
"""
print()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE friends (
# MAGIC  user_id INT,
# MAGIC  friend_id INT
# MAGIC );
# MAGIC INSERT INTO friends VALUES
# MAGIC (1, 2),(1, 3),
# MAGIC (1, 4),(2, 1),
# MAGIC (3, 1),(3, 4),
# MAGIC (4, 1),(4, 3);
# MAGIC
# MAGIC CREATE TABLE likes (
# MAGIC  user_id INT,
# MAGIC  page_id CHAR(1)
# MAGIC );
# MAGIC INSERT INTO likes VALUES
# MAGIC (1, 'A'),(1, 'B'),
# MAGIC (1, 'C'),(2, 'A'),
# MAGIC (3, 'B'),(3, 'C'),
# MAGIC (4, 'B');

# COMMAND ----------

"""
--A social media app tracks which users like which pages (hashtag#likes table).
--A friendship network is stored in the hashtag#friends table.
--The goal? Find pages liked by a user’s friend that the user hasn’t liked yet!
--Think of it as “Your friend liked this page, maybe you’d like it too!” 🎯
"""

import pyspark.sql.functions as F

df_friends = spark.read.table('friends')
df_likes = spark.read.table('likes')
df_frd_join = df_friends.join(df_likes,df_friends.friend_id==df_likes.user_id,'inner')\
    .select(df_friends['user_id'],df_friends['friend_id'],df_likes['page_id'])

df_frd_join.join(df_likes,(df_frd_join['user_id']==df_likes['user_id'])&(df_frd_join['page_id']==df_likes['page_id']),'leftanti')\
    .select(df_frd_join['user_id'],df_frd_join['page_id'])\
    .distinct()\
    .orderBy('user_id')\
    .display()

# df_friends.display()
# df_likes.display()

# COMMAND ----------

df_frd_join = df_friends.join(df_likes,df_friends.friend_id==df_likes.user_id,'inner')\
    .select(df_friends['user_id'],df_friends['friend_id'],df_likes['page_id'])

df_frd_join.join(df_likes,(df_frd_join['user_id']==df_likes['user_id'])&(df_frd_join['page_id']==df_likes['page_id']),'leftanti')\
    .select(df_frd_join['user_id'],df_frd_join['page_id'])\
    .distinct()\
    .orderBy('user_id')\
    .display()
    


# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(
# MAGIC select f.user_id,l.page_id as frd_liked from friends f  join likes l on f.friend_id == l.user_id
# MAGIC )
# MAGIC select * from cte c left join likes l on c.user_id = l.user_id
# MAGIC and c.frd_liked = l.page_id
# MAGIC -- where l.page_id is null
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as
# MAGIC (select f.user_id,l.page_id as frd_liked from friends f  join likes l on f.friend_id == l.user_id
# MAGIC )
# MAGIC select distinct c.user_id,c.frd_liked,l.page_id user_liked 
# MAGIC   from cte c join likes l on c.user_id == l.user_id
# MAGIC and l.page_id <> c.frd_liked;
# MAGIC -- where 