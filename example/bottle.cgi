#!/usr/bin/python
# import bottle web framework
from bottle import route, run, template, debug, jinja2_view
# used to set default template folder
import functools

# import the postgres module
import psycopg2

# import goodies to make SELECT statments eaiser
# Returns values from a SELECT as a dictionary. Yay!
from psycopg2.extras import DictCursor

# Connect to the database using the dictionary cursor
# Replace your database, username, and password
# conn = psycopg2.connect("dbname= user= password=")
conn = psycopg2.connect("dbname=shakespeare user= password=")

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
@view('works.html')
def home():
    # Select all of the works from our databse
    cur.execute("SELECT workid, longtitle, year, totalwords FROM work")
    # Fetch them all at once
    # We will give this list to the template so it can build a table
    work = cur.fetchall()
    # Render the template with all of the variables
    # The template expects a dictionary of values
    return {'works': work}

# Give details about a work
# URL format is work/<workid>
@route('/work/<workid>')
# set template file for this route (templates/title.html)
@view('title.html')
def work(workid=None):
    if workid:
        # Select all of the charaters from from the title
        cur.execute("SELECT c.charid, c.charname, c.description FROM character c, character_work cw where workid=%s AND cw.charid = c.charid", (workid,))
        chars = cur.fetchall()
        # Get information about the work
        cur.execute("SELECT workid, longtitle, year, totalwords, notes FROM work where workid = %s", (workid,))
        work = cur.fetchone()
        # Render the template with all of the variables
        return {'characters':chars, 'work':work}
    else:
        # We didn't get a workid
        home()

run(server='cgi')