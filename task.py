import subprocess
import time
import os

from datetime import datetime
from git import Repo


def push2github():
    repo = Repo(os.path.split(os.path.realpath(__file__))[0])
    remote = repo.remote()
    remote.pull()
    mod_count = 0
    for untracked_file in repo.untracked_files:
        if untracked_file.startswith("archive/"):
            repo.index.add(untracked_file)
            mod_count += 1
    for modified in repo.index.diff(None):
        if modified.a_path.startswith("archive/"):
            repo.index.add(modified.a_path)
            mod_count += 1
    if mod_count > 0:
        repo.index.commit("crawler auto commit")
        remote.push()


while True:
    # Run every hour
    now = datetime.now()
    if now.minute == 0:
        subprocess.call(["scrapy", "crawl", "trending"])
        # Commit every 3h
        if now.hour % 3 == 0:
            try:
                push2github()
            except:
                pass
        time.sleep(60 * 50)
    else:
        time.sleep(1)
