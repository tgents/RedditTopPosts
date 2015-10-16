#!/usr/bin/python
# import bottle web framework
from bottle import route, run, template, debug, jinja2_view
# used to set default template folder
import functools

# import the postgres module
import psycopg2
import psycopg2.extensions
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

# Library to handle unicode
import codecs

# import goodies to make SELECT statments eaiser
# Returns values from a SELECT as a dictionary. Yay!
from psycopg2.extras import DictCursor

# Connect to the database using the dictionary cursor
# Replace your database, username, and password
# conn = psycopg2.connect("dbname= user= password=")
conn = psycopg2.connect("dbname=tgents user=tgents password=mathso")

# Create a cursor
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# set default template folder to be 'templates'
view = functools.partial(jinja2_view, template_lookup=['templates'])

# Turn on debug mode, so errors are shown in the browser
debug(True)

# Give a list of works
# URL format is /
@route('/')
# set template file for this route (templates/works.html)
@view('home.html')
def home():
    # Select all of the works from our databse
    cur.execute("SELECT users.name as name, count(posts.post_id) as posts FROM users, posts WHERE posts.user_id = users.user_id GROUP BY users.name HAVING count(posts.post_id) >= 10")
    # Fetch them all at once
    # We will give this list to the template so it can build a table
    user = cur.fetchall()

    cur.execute("SELECT subreddits.sub_name as subs FROM posts, subreddits WHERE posts.subreddit_id = subreddits.sub_id GROUP BY subreddits.sub_name")
    subs = cur.fetchall()
    # Render the template with all of the variables
    # The template expects a dictionary of values
    return {'users': user, 'subs': subs}

# Give details about a work
# URL format is work/<workid>
@route('/user/<name>')
# set template file for this route (templates/title.html)
@view('userposts.html')
def user(name=None):
    if name:
        cur.execute("SELECT name FROM users WHERE name=%s", (name,))
        user = cur.fetchone()
        # Select all of the charaters from from the title
        cur.execute("SELECT posts.title as title, posts.score as score, posts.created_at as created, posts.permalink as comments, urls.url as url, subreddits.sub_name  FROM posts, users, subreddits, thumbnails, urls where users.name=%s AND posts.subreddit_id = subreddits.sub_id AND posts.thumbnail = thumbnails.thumb_id AND posts.user_id = users.user_id AND posts.url_id = urls.url_id", (name,))
        posts = cur.fetchall()
        # Render the template with all of the variables
        return {'posts': posts, 'user': user}
    else:
        # We didn't get a workid
        home()

@route('/subreddit/<sname>')
# set template file for this route (templates/title.html)
@view('subreddit.html')
def sub(sname=None):
    if sname:
        cur.execute("SELECT sub_name FROM subreddits WHERE sub_name=%s", (sname,))
        subreddit = cur.fetchone()
        # Select all of the charaters from from the title
        cur.execute("SELECT posts.title as title, posts.score as score, posts.created_at as created, posts.permalink as comments, urls.url as url, users.name as username  FROM posts, users, subreddits, urls where subreddits.sub_name=%s AND posts.subreddit_id = subreddits.sub_id AND posts.user_id = users.user_id AND posts.url_id = urls.url_id", (sname,))
        posts = cur.fetchall()
        # Render the template with all of the variables
        return {'posts': posts, 'subreddits': subreddit}
    else:
        # We didn't get a workid
        home()

run(server='cgi')