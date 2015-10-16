from __future__ import print_function

import psycopg2
import csv
import os
import time


conn = psycopg2.connect("dbname=tgents user=tgents password=mathso")
cur = conn.cursor()


#create tables
cur.execute("CREATE TABLE summary (num_posts int, num_users int, num_subreddits int, num_urls int)")
cur.execute("INSERT INTO summary VALUES (0,0,0,0)")
cur.execute("CREATE TABLE users (user_id serial PRIMARY KEY, name varchar)")
cur.execute("CREATE TABLE subreddits (sub_id varchar PRIMARY KEY, sub_name varchar)")
cur.execute("CREATE TABLE thumbnails (thumb_id serial PRIMARY KEY, thumb_link varchar)")
cur.execute("CREATE TABLE domains (domain_id serial PRIMARY KEY, domain_name varchar)")
cur.execute("CREATE TABLE urls (url_id serial PRIMARY KEY, url varchar, domain_id int REFERENCES domains(domain_id))")
cur.execute("CREATE TABLE user_types (type_id serial PRIMARY KEY, type varchar)")
cur.execute("CREATE TABLE distinguished_users (user_id int REFERENCES users(user_id), sub_id varchar  REFERENCES subreddits(sub_id), \
				type_id int REFERENCES user_types(type_id))")
cur.execute("CREATE TABLE posts (post_id varchar PRIMARY KEY, title varchar, score int, up_votes int, down_votes int, \
				is_self boolean, num_comments int, created_at timestamp, permalink varchar, \
				user_id int REFERENCES users(user_id), subreddit_id varchar REFERENCES subreddits(sub_id), \
				url_id int REFERENCES urls(url_id), thumbnail int REFERENCES thumbnails(thumb_id))")
cur.execute("CREATE TABLE self_text (post_id varchar REFERENCES posts(post_id), content text)")

#create functions
cur.execute("CREATE OR REPLACE FUNCTION addUser(uname varchar) RETURNS integer AS $$ \
				DECLARE \
				tmp int; \
				BEGIN \
				SELECT INTO tmp user_id FROM users WHERE name = uname; \
				IF tmp IS NULL THEN \
				INSERT INTO users (name) VALUES (uname); \
				SELECT INTO tmp user_id FROM users WHERE name = uname; \
				END IF; \
				RETURN tmp; \
				END; \
				$$ LANGUAGE plpgsql;")

cur.execute("CREATE OR REPLACE FUNCTION addSub(sid varchar, sname varchar) RETURNS varchar(10) AS $$ \
				DECLARE \
				tmp varchar; \
				BEGIN \
				SELECT INTO tmp sub_id FROM subreddits WHERE sub_id = sid; \
				IF tmp IS NULL THEN \
				INSERT INTO subreddits (sub_id, sub_name) VALUES (sid, sname); \
				SELECT INTO tmp sub_id FROM subreddits WHERE sub_id = sid; \
				END IF; \
				RETURN tmp; \
				END; \
				$$ LANGUAGE plpgsql;")

cur.execute("CREATE OR REPLACE FUNCTION addThumb(tlink varchar) RETURNS integer AS $$ \
				DECLARE \
				tmp int; \
				BEGIN \
				SELECT INTO tmp thumb_id FROM thumbnails WHERE thumb_link = tlink; \
				IF tmp IS NULL THEN \
				INSERT INTO thumbnails (thumb_link) VALUES (tlink); \
				SELECT INTO tmp thumb_id FROM thumbnails WHERE thumb_link = tlink; \
				END IF; \
				RETURN tmp; \
				END; \
				$$ LANGUAGE plpgsql;")

cur.execute("CREATE OR REPLACE FUNCTION addDomain(dom varchar) RETURNS integer AS $$ \
				DECLARE \
				tmp int; \
				BEGIN \
				SELECT INTO tmp domain_id FROM domains WHERE domain_name = dom; \
				IF tmp IS NULL THEN \
				INSERT INTO domains (domain_name) VALUES (dom); \
				SELECT INTO tmp domain_id FROM domains WHERE domain_name = dom; \
				END IF; \
				RETURN tmp; \
				END; \
				$$ LANGUAGE plpgsql;")

cur.execute("CREATE OR REPLACE FUNCTION addUrl(link varchar, dom varchar) RETURNS integer AS $$ \
				DECLARE \
				tmp int; \
				BEGIN \
				SELECT INTO tmp url_id FROM urls WHERE url = link; \
				IF tmp IS NULL THEN \
				INSERT INTO urls (url, domain_id) VALUES (link, addDomain(dom)); \
				SELECT INTO tmp url_id FROM urls WHERE url = link; \
				END IF; \
				RETURN tmp; \
				END; \
				$$ LANGUAGE plpgsql;")

cur.execute("CREATE OR REPLACE FUNCTION addUserType(utype varchar) RETURNS integer AS $$ \
				DECLARE \
				tmp int; \
				BEGIN \
				SELECT INTO tmp type_id FROM user_types WHERE type = utype; \
				IF tmp IS NULL THEN \
				INSERT INTO user_types (type) VALUES (utype); \
				SELECT INTO tmp type_id FROM user_types WHERE type = utype; \
				END IF; \
				RETURN tmp; \
				END; \
				$$ LANGUAGE plpgsql;")

#create triggers
cur.execute("CREATE OR REPLACE FUNCTION incrPosts() RETURNS trigger AS $$ \
				BEGIN UPDATE summary SET num_posts = num_posts+1; RETURN null; END $$ LANGUAGE plpgsql;")
cur.execute("CREATE TRIGGER update_post_sum AFTER INSERT ON posts EXECUTE PROCEDURE incrPosts()")

cur.execute("CREATE OR REPLACE FUNCTION incrUsers() RETURNS trigger AS $$ \
				BEGIN UPDATE summary SET num_users = num_users+1; RETURN null; END $$ LANGUAGE plpgsql;")
cur.execute("CREATE TRIGGER update_user_sum AFTER INSERT ON users EXECUTE PROCEDURE incrUsers()")

cur.execute("CREATE OR REPLACE FUNCTION incrSubs() RETURNS trigger AS $$ \
				BEGIN UPDATE summary SET num_subreddits = num_subreddits+1; RETURN null; END $$ LANGUAGE plpgsql;")
cur.execute("CREATE TRIGGER update_user_sum AFTER INSERT ON subreddits EXECUTE PROCEDURE incrSubs()")

cur.execute("CREATE OR REPLACE FUNCTION incrUrls() RETURNS trigger AS $$ \
				BEGIN UPDATE summary SET num_urls = num_urls+1; RETURN null; END $$ LANGUAGE plpgsql;")
cur.execute("CREATE TRIGGER update_url_sum AFTER INSERT ON urls EXECUTE PROCEDURE incrUrls();")

#insert data
for file in os.listdir("."):
    if file.endswith(".csv"):
        csvfile = open(file)
        datareader = csv.DictReader(csvfile, delimiter = ',')
        #print (datareader.fieldnames)

        for row in datareader:

        	subreddit_name = os.path.splitext(file)[0] #gets subreddit name from filename
        	subid = row['subreddit_id'] #gets subreddit id
        	time_created = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(float(row['created_utc']))) #converts the epoch code to a human date

        	cur.execute("SELECT addSub(%s, %s)", (subid, subreddit_name)) #adds subreddit

        	cur.execute("SELECT addUser('%s')" % row['author']) #adds user
        	userid = cur.fetchone() #fetchs user id

        	cur.execute("SELECT addThumb('%s')" % row['thumbnail']) #adds thumbnail
        	thumbid = cur.fetchone() #fetchs thumbnail id

        	cur.execute("SELECT addUrl(%s, %s)", (row['url'], row['domain'])) #adds url
        	urlid = cur.fetchone() #fetchs url id

        	#if the distinguished column marks the post with something like 'moderator', then it will add the user as a mod
        	if(row['distinguished'].strip() != ''): 
        		cur.execute("SELECT addUserType('%s')" % row['distinguished'])
        		utypeid = cur.fetchone()
        		cur.execute("SELECT user_id FROM distinguished_users WHERE user_id = %s" % userid)
        		blah = cur.fetchone()
        		if(blah is None):
        			cur.execute("INSERT INTO distinguished_users(user_id, sub_id, type_id) VALUES (%s, %s, %s)", (userid, subid, utypeid))

        	#inserts post
        	cur.execute("INSERT INTO posts(post_id, title, score, up_votes, down_votes, \
				is_self, num_comments, created_at, permalink, user_id, subreddit_id, url_id, thumbnail) \
        		VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (row['id'], row['title'], row['score'],
        		row['ups'], row['downs'], row['is_self'], row['num_comments'], time_created, row['permalink'], userid, subid, urlid, thumbid))

        	if(row['selftext'] != ''):
        		cur.execute("INSERT INTO self_text(post_id, content) VALUES (%s,%s)", (row['id'], row['selftext']))

        	print ("post %s inserted from %s" % (row['id'], subreddit_name))
        	#print "Inserted post %s", (row['id'])

        #print "Inserted subreddit %s" % file
        print ("%s done inserting" % file)

conn.commit()

cur.close()
conn.close()


