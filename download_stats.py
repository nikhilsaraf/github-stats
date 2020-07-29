#!/usr/bin/python

import datetime
import requests
import json
import psycopg2

global_host = "localhost"
global_db_user = ""
global_db_pass = ""
global_db_name = "kelp_download_stats"

def fetch_download_stats(repo_owner, repo_name, now):
    results = []
    x = requests.get("https://api.github.com/repos/{owner}/{name}/releases".format(owner=repo_owner, name=repo_name))
    for entry in x.json():
        release_name = entry['name']
        #print("{}".format(release_name))

        for asset in entry['assets']:
            asset_name = asset['name']
            download_count = asset['download_count']
            #print("    {} downloads - {}".format(download_count, asset_name))
            results.append((repo_owner, repo_name, release_name, asset_name, now, download_count))
    return results

def print_results(results):
    for r in results:
        print(r)

def write_stats_to_db(db_conn, rows):
    cur = db_conn.cursor()
    insert_statement = "INSERT INTO download_stats (repo_owner, repo_name, release_name, asset_name, capture_date_utc, count) VALUES"
    for i in range(len(rows)):
        row = rows[i]
        if i > 0:
            insert_statement += ","
        insert_statement += " ('{repo_owner}', '{repo_name}', '{release_name}', '{asset_name}', '{capture_date_utc}', '{count}')".format(repo_owner = row[0], repo_name = row[1], release_name = row[2], asset_name = row[3], capture_date_utc = row[4], count = row[5])

    cur.execute(insert_statement)
    db_conn.commit()
    cur.close()
    print('wrote stats for {} rows'.format(len(rows)))

def ensure_database(host, db_user, db_pass, db_name):
    db_conn = psycopg2.connect(host=host, user=db_user, password=db_pass)
    db_conn.autocommit = True
    cur = db_conn.cursor()

    try:
        cur.execute("CREATE DATABASE {}".format(db_name))
        print ('created database')
    except psycopg2.errors.DuplicateDatabase:
        print ('using existing database')
    finally:
        # do not need to commit db conn
        cur.close()
        db_conn.close()

def ensure_table(db_conn):
    cur = db_conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS download_stats (repo_owner TEXT NOT NULL, repo_name TEXT NOT NULL, release_name TEXT NOT NULL, asset_name TEXT NOT NULL, capture_date_utc TIMESTAMP WITHOUT TIME ZONE NOT NULL, count INTEGER NOT NULL, PRIMARY KEY(repo_owner, repo_name, release_name, asset_name, capture_date_utc))")
    db_conn.commit()
    cur.close()

if __name__ == "__main__":
    host = global_host
    db_user = global_db_user
    db_pass = global_db_pass
    db_name = global_db_name

    ensure_database(host, db_user, db_pass, db_name)
    db_conn = psycopg2.connect(host=host, user=db_user, password=db_pass, dbname=db_name)
    ensure_table(db_conn)

    repo_owner = "stellar"
    repo_name = "kelp"
    now = datetime.datetime.now()
    
    results = fetch_download_stats(repo_owner, repo_name, now)
    print_results(results)

    write_stats_to_db(db_conn, results)
    db_conn.close()

#
# Full Database Schema (only going with a single download_stats table for now: repo_owner, repo_name, release_name, asset_name, now, download_count)
#
# repos
#   - id ID
#   - repo_owner string
#   - repo_name string
# 
# releases
#   - id ID
#   - repo_id ID
#   - tag string
#   - name string
#   - created_at date
#
# assets
#   - id ID
#   - repo_id ID
#   - release_id ID
#   - name string
#   - created_at date
#
# stat_requests
#   - id ID
#   - repo_ids []ID
#   - date date
#
# download_stats
#   - id ID(repo_id, release_id, asset_id, request_id)
#   - repo_id ID
#   - release_id ID
#   - asset_id ID
#   - request_id ID
#   - download_count uint64

