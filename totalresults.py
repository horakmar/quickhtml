#!/usr/bin/python
# vim:fileencoding=utf-8:tabstop=4:shiftwidth=4:expandtab

############################################
#  Generate HTML total results from Jinja2 template
#  for multistage event
#
#  Author: Martin Horak
#  Version: 1.0
#  Date: 28. 6. 2021
#
############################################
import argparse
import pathlib
import sys, os, time
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, select_autoescape

## Constants ## ==========================
############### ==========================

## Functions ## ============================
############### ============================
def timefmt(milisecs):
    '''Format time in MS to [H]:MM:SS'''
    if milisecs == None:
        return ' -- '
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

    parser.add_argument("-d", "--html-dir", help="Directory where HTML pages will be stored [./html]", type=pathlib.Path, default="./html")

    parser.add_argument("-v", "--verbose", help="More information", action="count", default=1)
    parser.add_argument("-q", "--quiet", help="Less information", action="count", default=0)

    args = parser.parse_args()
    args.verbose -= args.quiet
    outdir = args.html_dir

    try:
        outdir.mkdir(parents=True, exist_ok=True)
    except OSError:
        print(f"Cannot create output directories ({outdir})")
        sys.exit(1)


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

    cur = dbcon.cursor()

    if is_bigdb:
        cur.execute("SET SCHEMA %s", (args.event,))

# Main loop
# Read event data
    cur.execute("SELECT ckey, cvalue FROM config WHERE ckey LIKE 'event.%'")
    event = {}
    for i in cur:
        (_, field) = i[0].split('.', 2)
        event[field] = i[1]

# Read classes list
    cur.execute(f"SELECT id, name FROM classes ORDER BY id")
    classes = []
    for i in cur:
        classes.append({'id': i[0], 'name': i[1]})

    env = Environment(loader=FileSystemLoader('templates'), autoescape=select_autoescape())
    results = {}
    for cls in classes:
        cur.execute(f"""
SELECT comp.registration, COALESCE(comp.lastName, '') || ' ' || COALESCE(comp.firstName, '') AS fullName,
  CASE WHEN e1.disqualified THEN NULL ELSE e1.timems END AS t1,
  CASE WHEN e2.disqualified THEN NULL ELSE e2.timems END AS t2,
  CASE WHEN e3.disqualified THEN NULL ELSE e3.timems END AS t3,
  CASE WHEN e4.disqualified THEN NULL ELSE e4.timems END AS t4,
  CASE WHEN e1.disqualified OR e2.disqualified OR e3.disqualified OR e4.disqualified THEN NULL
  ELSE e1.timems + e2.timems + e3.timems + e4.timems END
  AS total,
  e1.notcompeting OR e2.notcompeting OR e3.notcompeting OR e4.notcompeting AS notcompeting
FROM
  competitors AS comp,
  runs AS e1, runs AS e2, runs AS e3, runs AS e4
WHERE
  comp.classid = {placeholder} AND
  e1.competitorid = comp.id AND
  e2.competitorid = comp.id AND
  e3.competitorid = comp.id AND
  e4.competitorid = comp.id AND
  e1.stageid=1 AND e2.stageid=2 AND e3.stageid=3 AND e4.stageid=4
ORDER BY
  notcompeting, total, fullname
                    """, (cls['id'], ))
        competitors = []
        for i in cur:
            competitors.append({
                'registration': i[0],
                'fullname': i[1],
                'e1': timefmt(i[2]),
                'e2': timefmt(i[3]),
                'e3': timefmt(i[4]),
                'e4': timefmt(i[5]),
                'total': timefmt(i[6]),
                'notcompeting': i[7],
            })
        results[cls['id']] = competitors

    tmpl_total = env.get_template("results/total.html")
    tmpl_total.stream({'classes': classes, 'event': event, 'results': results, 'curtime': datetime.now()}).dump(f'{outdir}/total_results.html')

    print("Generated.")
## Main end =================================

## Main run =================================
########### =================================
if __name__ == '__main__':
    main()

## End of program # =========================
################### =========================
