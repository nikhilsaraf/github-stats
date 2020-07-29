#!/usr/bin/python

import datetime
import requests
import json

def fetch_download_stats(repo_owner, repo_name, now):
    results = []
    x = requests.get("https://api.github.com/repos/{owner}/{name}/releases".format(owner=repo_owner, name=repo_name))
    for entry in x.json():
        release_name = entry['name']
        print("{}".format(release_name))

        for asset in entry['assets']:
            asset_name = asset['name']
            download_count = asset['download_count']
            print("    {} downloads - {}".format(download_count, asset_name))
            results.append((repo_owner, repo_name, release_name, asset_name, now, download_count))
    return results

def print_results(results):
    for r in results:
        print(r)

if __name__ == "__main__":
    repo_owner = "stellar"
    repo_name = "kelp"
    now = datetime.datetime.now()
    
    results = fetch_download_stats(repo_owner, repo_name, now)
    print_results(results)

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

