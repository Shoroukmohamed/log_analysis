# "Database code" for the DB news. 

import psycopg2
DBNAME = "news"

# database quiries with the connection
# query 1:
# what are the three most popular articles of all time?
def give_top_three_articles():
    conn = psycopg2.connect(database=DBNAME)
    cn=conn.cursor()
    cn.execute("""select articles.title, count(*) as num
                  from articles join log on
                  articles.slug=substr(log.path, 10)
                  group by articles.title
                  order by num desc limit 3""")
    return cn.fetchall()
    conn.close()


# query 2:
# who are most popular article authors of all time?
def give_pop_art_auth():
    conn = psycopg2.connect(database=DBNAME)
    cn=conn.cursor()
    cn.execute("""select authors.name, count(*) as num
                  from articles join authors on
                  authors.id=articles.author
                  join log on articles.slug = substr(log.path, 10)
                  group by authors.name order by num desc;""")
    return cn.fetchall()
    conn.close()

# query 3:
# On which day did more than 1% of requests lead to errors?
def define_error_req():
    conn = psycopg2.connect(database=DBNAME)
    cn=conn.cursor()
    cn.execute("""select * from (select date(time),round(100.0*sum(case log.status
                  when '200 OK'  then 0 else 1 end)/count(log.status),3) as error from log group
                  by date(time) order by error desc) as subq where error > 1;""")
    return cn.fetchall()
    conn.close()
    
# show the three most popular articles of all time
def disp_pop_articles():
    pop_articles = give_top_three_articles()
    print("popular articles")
    for name, num in pop_articles:
        print(" \"{}\" -- {} views".format(name, num))


# show popular article authors of all time
def disp_pop_articles_author():
    pop_articles_authors = give_pop_art_auth()
    print("popular articles authors")
    for name, num in pop_articles_authors:
        print(" {} -- {} views".format(name, num))


# which day did more than 1% of requests lead to errors
def disp_error_req():
    error_req = define_error_req()
    print("day did more than 1% of requests lead to errors")
    for day, percentagefailed in error_req:
        print("{0:%B %d, %Y} -- {1:.2f} % errors".format(day, percentagefailed))


if __name__ == '__main__':
     disp_pop_articles()
     disp_pop_articles_author()
     disp_error_req()
    








    
