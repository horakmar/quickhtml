#!/usr/bin/python
# vim:fileencoding=utf-8:tabstop=4:shiftwidth=4:expandtab

############################################
#  Generate HTML results from Jinja2 template
#
#  Author: Martin Horak
#  Version: 1.0
#  Date: 22. 6. 2021
#
############################################
import argparse
import pathlib
import sys, os, time
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, select_autoescape

## Constants ## ==========================
############### ==========================
# Translate national characters to ASCII for file names.
tr1 = 'áčďéěíňóřšťůúýžľĺÁČĎÉĚÍŇÓŘŠŤŮÚÝŽĽĹ'
tr2 = 'acdeeinorstuuyzllacdeeinorstuuyzll'
trans = str.maketrans(tr1, tr2)


## Functions ## ============================
############### ============================
def timefmt(milisecs):
    '''Format time in MS to [H]:MM:SS'''
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

## Main ## =================================
########## =================================
def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("event", help="Event - name of DB schema (psql) or filename (sqlite)")

    parser.add_argument("--sql-driver", help="SQL database to connect (psql|sqlite) [psql]", default="psql", choices=['psql', 'sqlite'])
    parser.add_argument("-s", "--sql-server", help="Server to connect [localhost]", default="localhost")
    parser.add_argument("--sql-port", help="TCP port to connect [5432]", type=int, default=5432)

    parser.add_argument("-u", "--user", help="User for DB connection [quickevent]", default="quickevent")
    parser.add_argument("-p", "--password", help="Password for DB connection [None]")

    parser.add_argument("-b", "--sql-database", help="Database name [quickevent]", default="quickevent")
    parser.add_argument("-n", "--stage", help="Stage number [1]", type=int, default=1)

    parser.add_argument("-m", "--mode", help="Output mode (results|startlists|all) [all]", default="all", choices=['results', 'startlists', 'starts', 'all', 'r', 's', 'a'])
    parser.add_argument("--main-index", help="Create main index file [Automatically on in 'all' mode]", action='store_true')

    parser.add_argument("-d", "--html-dir", help="Directory where HTML pages will be stored [./html]", type=pathlib.Path, default="./html")
    parser.add_argument("-r", "--refresh-interval", help="Refresh time interval in seconds [60]", type=int, default=60)
    parser.add_argument("--classes-like", help='SQL LIKE expression to filter classes, e.g., --classes-like "M%%"')
    parser.add_argument("--classes-not-like", help='SQL LIKE expression to filter OUT classes, e.g., --classes-not-like "HDR"')

    parser.add_argument("-v", "--verbose", help="More information", action="count", default=1)
    parser.add_argument("-q", "--quiet", help="Less information", action="count", default=0)

    args = parser.parse_args()
    args.verbose -= args.quiet
    stage = args.stage
    outdir = args.html_dir.joinpath(f'E{stage}')
    mode = []
    main_index = args.main_index
    if args.mode in ('all', 'a'):
        mode.append('s')
        mode.append('r')
        main_index = True
    if args.mode in ('results', 'r'):
        mode.append('r')
    if args.mode in ('startlists', 'starts', 's'):
        mode.append('s')
# Connect to database
    try:
        if args.sql_driver == "psql":
            from psycopg2 import connect
            dbcon = connect(host=args.sql_server, database=args.sql_database, user=args.user, password=args.password)
            is_bigdb = True
            placeholder = "%s"
        else:
            from sqlite3 import connect
            dbcon = connect(database=args.event)
            is_bigdb = False
            placeholder = "?"

    except OperationalError:
        print("Cannot connect to database.")
        sys.exit(1)

    try:
        outdir.joinpath('results').mkdir(parents=True, exist_ok=True)
        outdir.joinpath('starts').mkdir(parents=True, exist_ok=True)
    except OSError:
        print(f"Cannot create output directories ({outdir})")
        sys.exit(1)

    cur = dbcon.cursor()

    if is_bigdb:
        cur.execute("SET SCHEMA %s", (args.event,))

# Main loop
    while True:
# Read event data
        cur.execute("SELECT ckey, cvalue FROM config WHERE ckey LIKE 'event.%'")
        event = {}
        for i in cur:
            (_, field) = i[0].split('.', 2)
            event[field] = i[1]

# Read classes list
        cur.execute(f"SELECT classes.id, name FROM classes INNER JOIN classdefs ON classdefs.classId=classes.id AND (classdefs.stageId={placeholder}) ORDER BY name", (stage,))
        classes = []
        for i in cur:
            classes.append({'id': i[0], 'name': i[1], 'ascii': i[1].translate(trans).lower()})

        env = Environment(loader=FileSystemLoader('templates'), autoescape=select_autoescape())

# Generate main index file
        if main_index:
            tmpl_index = env.get_template("index.html")
            tmpl_index.stream({'classes': classes, 'event': event, 'stage': stage}).dump(f'{outdir}/index.html')

        if 'r' in mode:
            tmpl_index = env.get_template("results/index.html")
            tmpl_index.stream({'classes': classes, 'event': event, 'stage': stage}).dump(f'{outdir}/results/index.html')
        if 's' in mode:
            tmpl_index = env.get_template("startlists/index.html")
            tmpl_index.stream({'classes': classes, 'event': event, 'stage': stage}).dump(f'{outdir}/starts/index.html')

        for cls in classes:
            cur.execute(f"SELECT classes.name, courses.length, courses.climb FROM classes LEFT JOIN classdefs ON classdefs.classId=classes.id AND (classdefs.stageId={placeholder}) INNER JOIN courses ON courses.id=classdefs.courseId WHERE (classes.id={placeholder})", (stage, cls['id']))
            r = cur.fetchone()
            cls['length'] = r[1]
            cls['climb'] = r[2]

# Read results
            if 'r' in mode:
                cur.execute(f"SELECT competitors.registration, competitors.lastName, competitors.firstName, COALESCE(competitors.lastName, '') || ' ' || COALESCE(competitors.firstName, '') AS fullName, runs.siid, runs.leg, runs.relayid, runs.checktimems, runs.starttimems, runs.finishtimems, runs.penaltytimems, runs.timems, runs.notcompeting, runs.disqualified, runs.mispunch, runs.badcheck FROM competitors JOIN runs ON runs.competitorId=competitors.id AND (runs.stageId={placeholder} AND runs.isRunning AND runs.finishTimeMs>0) WHERE (competitors.classId={placeholder}) ORDER BY runs.notCompeting, runs.disqualified, runs.timeMs", (stage, cls['id']))

                filename = cls['ascii']

                competitors = []
                for i in cur:

                    competitors.append({
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

                tmpl_class = env.get_template("results/class.html")
                tmpl_class.stream({'classes': classes, 'cls': cls, 'event': event, 'stage': stage, 'competitors': competitors, 'curtime': datetime.now()}).dump(f'{outdir}/results/{filename}.html')

# Read startlists
            if 's' in mode:
                cur.execute(f"SELECT startdatetime FROM stages WHERE (id={placeholder})", (stage, ))

                start_dt = cur.fetchone()[0]

                cur.execute(f"SELECT competitors.registration, competitors.lastName, competitors.firstName, COALESCE(competitors.lastName, '') || ' ' || COALESCE(competitors.firstName, '') AS fullName, runs.siid, runs.leg, runs.relayid, runs.starttimems, runs.notcompeting FROM competitors JOIN runs ON runs.competitorId=competitors.id AND runs.stageId={placeholder} WHERE (competitors.classId={placeholder}) ORDER BY runs.starttimems, fullName", (stage, cls['id']))

                filename = cls['ascii']

                competitors = []
                for i in cur:

                    competitors.append({
                        'registration': i[0],
                        'lastname': i[1],
                        'firstname': i[2],
                        'fullname': i[3],
                        'siid': i[4],
                        'leg': i[5],
                        'relayid': i[6],
                        'starttime': timefmt(i[7]),
                        'notcompeting': i[8],
                    })
                tmpl_class = env.get_template("startlists/class.html")
                tmpl_class.stream({'classes': classes, 'cls': cls, 'event': event, 'stage': stage, 'competitors': competitors, 'start_dt': start_dt, 'curtime': datetime.now()}).dump(f'{outdir}/starts/{filename}.html')

        print("Generated.")
        if args.refresh_interval == 0:
            break
        time.sleep(args.refresh_interval)
## Main end =================================

## Main run =================================
########### =================================
if __name__ == '__main__':
    main()

## End of program # =========================
################### =========================

# SELECT classes.name AS classes__name, courses.length AS courses__length, courses.climb AS courses__climb FROM classes LEFT JOIN classdefs ON classdefs.classId=classes.id AND (classdefs.stageId=1) LEFT JOIN courses ON courses.id=classdefs.courseId WHERE (classes.id=124208)

# SELECT competitors.registration AS competitors__registration, competitors.lastName AS competitors__lastName, competitors.firstName AS competitors__firstName, COALESCE(competitors.lastName, '') || ' ' || COALESCE(competitors.firstName, '') AS competitorName, runs.id AS runs__id, runs.competitorid AS runs__competitorid, runs.siid AS runs__siid, runs.stageid AS runs__stageid, runs.leg AS runs__leg, runs.relayid AS runs__relayid, runs.checktimems AS runs__checktimems, runs.starttimems AS runs__starttimems, runs.finishtimems AS runs__finishtimems, runs.penaltytimems AS runs__penaltytimems, runs.timems AS runs__timems, runs.isrunning AS runs__isrunning, runs.notcompeting AS runs__notcompeting, runs.disqualified AS runs__disqualified, runs.mispunch AS runs__mispunch, runs.badcheck AS runs__badcheck, runs.cardlent AS runs__cardlent, runs.cardreturned AS runs__cardreturned, runs.importid AS runs__importid FROM competitors JOIN runs ON runs.competitorId=competitors.id AND (runs.stageId=1 AND runs.isRunning AND runs.finishTimeMs>0) WHERE (competitors.classId=124211) ORDER BY runs.notCompeting, runs.disqualified, runs.timeMs
