#!/usr/bin/python
import sys, os
from jinja2 import Environment, FileSystemLoader, select_autoescape
from psycopg2 import connect

# Translate national characters to ASCII for file names.
tr1 = 'áčďéěíňóřšťůúýžľĺÁČĎÉĚÍŇÓŘŠŤŮÚÝŽĽĹ'
tr2 = 'acdeeinorstuuyzllacdeeinorstuuyzll'

trans = str.maketrans(tr1, tr2)


def timefmt(milisecs):
    if milisecs == None:
        return '--'
    secs = int(milisecs / 1000)
    hours = secs // 3600
    mins = (secs - hours * 3600) // 60
    secs = secs % 60
    if hours > 0:
        out = f'{hours}:'
    else:
        out = ''
    out += f'{mins:02}:'
    out += f'{secs:02}'
    return out

# Connect to database
try:
    dbcon = connect(host="localhost", database="quickevent", user="quickevent", password="OstSud2021")
except OperationalError:
    print("Cannot connect to database.")
    sys.exit(1)


eventid = 'mtbo_long'
stage = 1
outdir = '/var/www/html/results'

try:
    if not os.path.exists(outdir):
        os.makedirs(outdir)
except OSError:
    print(f"Cannot create output directory ({outdir})")
    sys.exit(1)

cur = dbcon.cursor()

cur.execute("SET SCHEMA %s", (eventid,))
# Read event data
cur.execute("SELECT ckey, cvalue FROM config WHERE ckey LIKE 'event.%'")
event = {}
for i in cur:
    (_, field) = i[0].split('.', 2)
    event[field] = i[1]

# Read classes list
cur.execute("SELECT classes.id, name FROM classes INNER JOIN classdefs ON classdefs.classId=classes.id AND (classdefs.stageId=%s) ORDER BY name", (stage,))
classes = []
for i in cur:
    classes.append({'id': i[0], 'name': i[1], 'ascii': i[1].translate(trans).lower()})

env = Environment(loader=FileSystemLoader('templates'), autoescape=select_autoescape())
tmpl_index = env.get_template("index.html")

tmpl_index.stream({'classes': classes, 'event': event, 'stage': stage}).dump(f'{outdir}/index.html')

for cls in classes:
    cur.execute("SELECT classes.name, courses.length, courses.climb FROM classes LEFT JOIN classdefs ON classdefs.classId=classes.id AND (classdefs.stageId=%s) INNER JOIN courses ON courses.id=classdefs.courseId WHERE (classes.id=%s)", (stage, cls['id']))
    r = cur.fetchone()
    cls['length'] = r[1]
    cls['climb'] = r[2]

    cur.execute("SELECT competitors.registration, competitors.lastName, competitors.firstName, COALESCE(competitors.lastName, '') || ' ' || COALESCE(competitors.firstName, '') AS fullName, runs.siid, runs.leg, runs.relayid, runs.checktimems, runs.starttimems, runs.finishtimems, runs.penaltytimems, runs.timems, runs.notcompeting, runs.disqualified, runs.mispunch, runs.badcheck FROM competitors JOIN runs ON runs.competitorId=competitors.id AND (runs.stageId=%s AND runs.isRunning AND runs.finishTimeMs>0) WHERE (competitors.classId=%s) ORDER BY runs.notCompeting, runs.disqualified, runs.timeMs", (stage, cls['id']))

    results = []
    for i in cur:

        results.append({
            'registration': i[0],
            'lastname': i[1],
            'firstname': i[2],
            'fullname': i[3],
            'siid': i[4],
            'leg': i[5],
            'relayid': i[6],
            'checktime': timefmt(i[7]),
            'starttime': timefmt(i[8]),
            'finishtime': timefmt(i[9]),
            'penaltytime': timefmt(i[10]),
            'time': timefmt(i[11]),
            'notcompeting': i[12],
            'disq': i[13],
            'mispunch': i[14],
            'badcheck': i[15]
        })

    filename = cls['ascii']
    tmpl_class = env.get_template("class.html")
    tmpl_class.stream({'classes': classes, 'cls': cls, 'event': event, 'stage': stage, 'results': results}).dump(f'{outdir}/{filename}.html')

# SELECT classes.name AS classes__name, courses.length AS courses__length, courses.climb AS courses__climb FROM classes LEFT JOIN classdefs ON classdefs.classId=classes.id AND (classdefs.stageId=1) LEFT JOIN courses ON courses.id=classdefs.courseId WHERE (classes.id=124208)

# SELECT competitors.registration AS competitors__registration, competitors.lastName AS competitors__lastName, competitors.firstName AS competitors__firstName, COALESCE(competitors.lastName, '') || ' ' || COALESCE(competitors.firstName, '') AS competitorName, runs.id AS runs__id, runs.competitorid AS runs__competitorid, runs.siid AS runs__siid, runs.stageid AS runs__stageid, runs.leg AS runs__leg, runs.relayid AS runs__relayid, runs.checktimems AS runs__checktimems, runs.starttimems AS runs__starttimems, runs.finishtimems AS runs__finishtimems, runs.penaltytimems AS runs__penaltytimems, runs.timems AS runs__timems, runs.isrunning AS runs__isrunning, runs.notcompeting AS runs__notcompeting, runs.disqualified AS runs__disqualified, runs.mispunch AS runs__mispunch, runs.badcheck AS runs__badcheck, runs.cardlent AS runs__cardlent, runs.cardreturned AS runs__cardreturned, runs.importid AS runs__importid FROM competitors JOIN runs ON runs.competitorId=competitors.id AND (runs.stageId=1 AND runs.isRunning AND runs.finishTimeMs>0) WHERE (competitors.classId=124211) ORDER BY runs.notCompeting, runs.disqualified, runs.timeMs
