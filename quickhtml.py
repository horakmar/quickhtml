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
def timefmt(milisecs, show_hours=False):
    '''Format time in MS to [H]:MM:SS'''
    if milisecs == None:
        return '--'
    secs = int(milisecs / 1000)
    if show_hours:
        hours = secs // 3600
    else:
        hours = 0
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

    modes = ['r', 's', 't', 'a']

    parser = argparse.ArgumentParser()

    parser.add_argument("event", help="Event - name of DB schema (psql) or filename (sqlite)")

    parser.add_argument("--sql-driver", help="SQL database to connect (psql|sqlite) [psql]", default="psql", choices=['psql', 'sqlite'])
    parser.add_argument("-s", "--sql-server", help="Server to connect [localhost]", default="localhost")
    parser.add_argument("--sql-port", help="TCP port to connect [5432]", type=int, default=5432)

    parser.add_argument("-u", "--user", help="User for DB connection [quickevent]", default="quickevent")
    parser.add_argument("-p", "--password", help="Password for DB connection [None]")

    parser.add_argument("-b", "--sql-database", help="Database name [quickevent]", default="quickevent")
    parser.add_argument("-n", "--stage", help="Stage number [1]", type=int, default=0)

    parser.add_argument("-m", "--mode", help="Output mode (results|starts|total)", choices=modes, action='append')
    parser.add_argument("--main-index", help="Create main index file [Automatically on in 'all' mode]", action='store_true')

    parser.add_argument("-d", "--html-dir", help="Directory where HTML pages will be stored [./html]", type=pathlib.Path, default="./html")
    parser.add_argument("-r", "--refresh-interval", help="Refresh time interval in seconds [60]", type=int, default=60)

    parser.add_argument("-H", "--show-no-hours", help="Format results as MMM:SS [default H:MM:SS]", action='store_true')

    parser.add_argument("-v", "--verbose", help="More information", action="count", default=1)
    parser.add_argument("-q", "--quiet", help="Less information", action="count", default=0)

    args = parser.parse_args()
    args.verbose -= args.quiet
    mode = []
    main_index = args.main_index
    show_hours = not args.show_no_hours
    if args.mode == None or 'a' in args.mode:
        mode.append('s')
        mode.append('r')
        main_index = True
        if 'a' in args.mode:
            mode.append('t')
    else:
        mode = args.mode


# Connect to database
    try:
        if args.sql_driver == "psql":
            from psycopg2 import connect
            dbcon = connect(host=args.sql_server, database=args.sql_database, user=args.user, password=args.password)
            is_bigdb = True
            plc = "%s"
        else:
            from sqlite3 import connect
            dbcon = connect(database=args.event)
            is_bigdb = False
            plc = "?"

    except OperationalError:
        print("Cannot connect to database.")
        sys.exit(1)


# Initialize DB connection
    cur = dbcon.cursor()

    if is_bigdb:
        cur.execute(f"SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname={plc}",
                    (args.event,))
        if cur.fetchone()[0] != 1:
            print(f"Schema {args.event} doesn't exist.")
            sys.exit(1)
        cur.execute("SET SCHEMA %s", (args.event,))

# Initialize Jinja templates
    env = Environment(loader=FileSystemLoader('templates'), autoescape=select_autoescape())

# Create dir for total results
    if 't' in mode:
        outdir_total = args.html_dir.joinpath(f'total')
        try:
            outdir_total.mkdir(parents=True, exist_ok=True)
        except OSError:
            print(f"Cannot create output directories ({outdir})")
            sys.exit(1)


# Main loop
    while True:
# Read event data
        cur.execute("SELECT ckey, cvalue FROM config WHERE ckey LIKE 'event.%'")
        event = {}
        for i in cur:
            (_, field) = i[0].split('.', 2)
            event[field] = i[1]
        if args.stage > 0:
            stage = min(args.stage, int(event['stageCount']))
        else:
            stage = int(event['currentStageId'])
        outdir = args.html_dir.joinpath(f'E{stage}')
        try:
            outdir.joinpath('results').mkdir(parents=True, exist_ok=True)
            outdir.joinpath('starts').mkdir(parents=True, exist_ok=True)
        except OSError:
            print(f"Cannot create output directories ({outdir})")
            sys.exit(1)

# Read classes list
        cur.execute(f"SELECT classes.id, name FROM classes INNER JOIN classdefs ON classdefs.classId=classes.id AND (classdefs.stageId={plc}) ORDER BY name", (stage,))
        classes = []
        for i in cur:
            classes.append({'id': i[0], 'name': i[1], 'ascii': i[1].translate(trans).lower()})

# Generate main index file
        if main_index:
            tmpl_index = env.get_template("index.html")
            tmpl_index.stream({'classes': classes, 'event': event, 'stage': stage}).dump(f'{outdir}/index.html')

# Generate separate index files
        if 'r' in mode:
            tmpl_index = env.get_template("results/index.html")
            tmpl_index.stream({'classes': classes, 'event': event, 'stage': stage}).dump(f'{outdir}/results/index.html')
        if 's' in mode:
            tmpl_index = env.get_template("startlists/index.html")
            tmpl_index.stream({'classes': classes, 'event': event, 'stage': stage}).dump(f'{outdir}/starts/index.html')
        if 't' in mode:
            tmpl_index = env.get_template("total/index.html")
            tmpl_index.stream({'classes': classes, 'event': event, 'stage': stage}).dump(f'{outdir_total}/index.html')

# Classes loop
        for cls in classes:
            cur.execute(f"SELECT classes.name, courses.length, courses.climb FROM classes LEFT JOIN classdefs ON classdefs.classId=classes.id AND (classdefs.stageId={plc}) INNER JOIN courses ON courses.id=classdefs.courseId WHERE (classes.id={plc})", (stage, cls['id']))
            r = cur.fetchone()
            cls['length'] = r[1]
            cls['climb'] = r[2]

# Count total results
            if 't' in mode:
# Read competitors
# totals: { id: [reg, name, total_time, race_stat, notcompeting, [E1_time, E1_status, E1_notcomp, E1_rank], [E2_time, ...]] }
                cur.execute(f"""
SELECT
  id, registration,
  COALESCE(lastName, '') || ' ' || COALESCE(firstName, '') AS fullName
FROM competitors
WHERE
  classid = {plc}
ORDER BY id
                            """, (cls['id'], ))
                totals = {}
                for i in cur:
                    totals[i[0]] = [i[1], i[2], 0, 0, False]


                for stg in range(1, stage+1):
                    cur.execute(f"""
SELECT
  comp.id,
  e.timems,
  e.isrunning, e.disqualified, e.notcompeting
FROM
  competitors AS comp,
  runs as e
WHERE
  comp.classid = {plc} AND
  comp.id = e.competitorid AND
  e.stageid = {plc}
ORDER BY
  e.notcompeting, not(e.isrunning), e.disqualified,
  e.timems
                                """, (cls['id'], stg))
                    rank = 0
                    for i in cur:
                        rank += 1
                        cid = i[0]
                        status = 0
                        if not i[2]:
                            status = 2 # DNS
                        elif i[3]:
                            status = 1 # DISQ
                        elif i[1] == None:
                            status = 3 # DNF

                        stg_res = [i[1], status, rank]

                        # Add time to total
                        if status == 0:
                            totals[cid][2] += i[1]
                        else:
                            totals[cid][3] += status
                        totals[cid][4] |= i[4]
                        totals[cid].append(stg_res)

                results = []
                for i in sorted(totals.values(), key=lambda x: (x[4], x[3], x[2])):
                    stages = []
                    for j in range(1, stage+1):
                        rank = '--'
                        time = '--'
                        if i[j+4][1] == 0:
                            time = timefmt(i[j+4][0], show_hours)
                            if not i[4]:
                                rank = i[j+4][2]
                        elif i[j+4][1] == 1:
                            time = 'DISK'
                        elif i[j+4][1] == 2:
                            time = 'NEST'
                        elif i[j+4][1] == 3:
                            time = 'NEDO'
                        stages.append({
                            'time': time,
                            'rank': rank
                        })

                    results.append({
                            'registration': i[0],
                            'fullname': i[1],
                            'totaltime': timefmt(i[2], show_hours),
                            'racestat': i[3],
                            'notcompeting': i[4],
                            'stages': stages
                    })

                filename = cls['ascii']
                tmpl_class = env.get_template("total/class.html")
                tmpl_class.stream({'classes': classes, 'cls': cls, 'event': event, 'stage': stage, 'results': results, 'curtime': datetime.now()}).dump(f'{outdir_total}/{filename}.html')

# Read results
            if 'r' in mode:
                cur.execute(f"SELECT competitors.registration, competitors.lastName, competitors.firstName, COALESCE(competitors.lastName, '') || ' ' || COALESCE(competitors.firstName, '') AS fullName, runs.siid, runs.leg, runs.relayid, runs.checktimems, runs.starttimems, runs.finishtimems, runs.penaltytimems, runs.timems, runs.notcompeting, runs.disqualified, runs.mispunch, runs.badcheck FROM competitors JOIN runs ON runs.competitorId=competitors.id AND (runs.stageId={plc} AND runs.isRunning AND runs.finishTimeMs>0) WHERE (competitors.classId={plc}) ORDER BY runs.notCompeting, runs.disqualified, runs.timeMs", (stage, cls['id']))

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
                        'checktime': timefmt(i[7], show_hours),
                        'starttime': timefmt(i[8], show_hours),
                        'finishtime': timefmt(i[9], show_hours),
                        'penaltytime': timefmt(i[10], show_hours),
                        'time': timefmt(i[11], show_hours),
                        'notcompeting': i[12],
                        'disq': i[13],
                        'mispunch': i[14],
                        'badcheck': i[15]
                    })

                filename = cls['ascii']
                tmpl_class = env.get_template("results/class.html")
                tmpl_class.stream({'classes': classes, 'cls': cls, 'event': event, 'stage': stage, 'competitors': competitors, 'curtime': datetime.now()}).dump(f'{outdir}/results/{filename}.html')

# Read startlists
            if 's' in mode:
                cur.execute(f"SELECT startdatetime FROM stages WHERE (id={plc})", (stage, ))

                start_dt = cur.fetchone()[0]

                cur.execute(f"SELECT competitors.registration, competitors.lastName, competitors.firstName, COALESCE(competitors.lastName, '') || ' ' || COALESCE(competitors.firstName, '') AS fullName, runs.siid, runs.leg, runs.relayid, runs.starttimems, runs.notcompeting FROM competitors JOIN runs ON runs.competitorId=competitors.id AND runs.stageId={plc} WHERE (competitors.classId={plc}) ORDER BY runs.starttimems, fullName", (stage, cls['id']))

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
                        'starttime': timefmt(i[7], show_hours),
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

# SELECT comp.registration, COALESCE(comp.lastName, '') || ' ' || COALESCE(comp.firstName, '') AS fullName,
  # CASE WHEN e1.disqualified THEN NULL ELSE e1.timems END AS t1,
  # concat_ws(', ', CASE WHEN e1.notcompeting THEN 'MS' END, CASE WHEN NOT e1.isrunning THEN 'DNS' END, CASE WHEN e1.disqualified THEN 'DISK' END) AS e1stat,
  # CASE WHEN e2.disqualified THEN NULL ELSE e2.timems END AS t2,
  # concat_ws(', ', CASE WHEN e2.notcompeting THEN 'MS' END, CASE WHEN NOT e2.isrunning THEN 'DNS' END, CASE WHEN e2.disqualified THEN 'DISK' END) AS e2stat,
  # CASE WHEN e1.disqualified OR e2.disqualified THEN NULL
  # ELSE e1.timems + e2.timems END
  # AS total,
  # e1.notcompeting OR e2.notcompeting AS notcompeting
# FROM
  # competitors AS comp,
  # runs AS e1, runs AS e2
# WHERE
  # comp.classid = 131942 AND
  # e1.competitorid = comp.id AND
  # e2.competitorid = comp.id AND
  # e1.stageid=1 AND e2.stageid=2
# ORDER BY
  # notcompeting, total


# SELECT classes.name AS classes__name, courses.length AS courses__length, courses.climb AS courses__climb FROM classes LEFT JOIN classdefs ON classdefs.classId=classes.id AND (classdefs.stageId=1) LEFT JOIN courses ON courses.id=classdefs.courseId WHERE (classes.id=124208)

# SELECT competitors.registration AS competitors__registration, competitors.lastName AS competitors__lastName, competitors.firstName AS competitors__firstName, COALESCE(competitors.lastName, '') || ' ' || COALESCE(competitors.firstName, '') AS competitorName, runs.id AS runs__id, runs.competitorid AS runs__competitorid, runs.siid AS runs__siid, runs.stageid AS runs__stageid, runs.leg AS runs__leg, runs.relayid AS runs__relayid, runs.checktimems AS runs__checktimems, runs.starttimems AS runs__starttimems, runs.finishtimems AS runs__finishtimems, runs.penaltytimems AS runs__penaltytimems, runs.timems AS runs__timems, runs.isrunning AS runs__isrunning, runs.notcompeting AS runs__notcompeting, runs.disqualified AS runs__disqualified, runs.mispunch AS runs__mispunch, runs.badcheck AS runs__badcheck, runs.cardlent AS runs__cardlent, runs.cardreturned AS runs__cardreturned, runs.importid AS runs__importid FROM competitors JOIN runs ON runs.competitorId=competitors.id AND (runs.stageId=1 AND runs.isRunning AND runs.finishTimeMs>0) WHERE (competitors.classId=124211) ORDER BY runs.notCompeting, runs.disqualified, runs.timeMs
