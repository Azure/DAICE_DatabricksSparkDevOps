
import argparse
import itertools
import json
import os
from pathlib import posixpath
import re
import subprocess

"""
WARNING: This is an in-elegant way of stopping a job in Databricks.  

Do NOT use this code on production systems.
"""


def parse_runs_list(cli_output):
    """
    Parse Databricks CLI output of `databricks runs list` to return
    a list of run ids, their names, and job ids.
    """
    runs = cli_output.decode('utf-8').replace('\r\n','\n').split('\n')
    output = {}
    for run in runs:
        # run_id, name, run_state, job_state, url
        matches = re.search('^(\d+) {2,}(.*?) {2,}(.*?) {2,}(.*?) {2,}(.*?)$', run)
        if matches and matches.group(3) !="TERMINATED":
            url_match = re.search(r'#job/(\d+)/run', matches.group(5))
            
            job_id = int(url_match.group(1))
            job_name = matches.group(2)
            run_id = matches.group(1)

            job_key = (job_id, job_name)

            output[job_key] = output.get(job_key, []) + [run_id]

    return output



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Kill a Databricks Spark job by name or job id"
    )
    parser.add_argument('--name',
        default=None,
        help="Name of the job you want to look for"
    )
    parser.add_argument('--id',
        default=None,
        help="Id of the job you want to look for"
    )
    parser.add_argument('--profile',
        default=None,
        help="Profile name to be passed to the databricks CLI"
    )
    
    args = parser.parse_args()
    
    PROFILE_EXTENSION = ['--profile', args.profile] if args.profile else []

    runs_output = subprocess.run(
            ['databricks', 'runs', 'list'] + PROFILE_EXTENSION, 
            stdout=subprocess.PIPE
    ).stdout

    runs_by_job = parse_runs_list(runs_output)
    print("There are {} jobs being considered".format(len(runs_by_job)))

    if args.id:
        runs = [runs for (job_id, job_name), runs in runs_by_job.items() if args.id == job_id]
    elif args.name:
        runs = [runs for (job_id, job_name), runs in runs_by_job.items() if args.name == job_name]
    
    runs = list(itertools.chain.from_iterable(runs))

    if len(runs) == 0:
        print("No runs to cancel")
        exit(0)

    for run in runs:
        print("Cancelling run: {}".format(run))
        _ = subprocess.run(
            ['databricks', 'runs', 'cancel', '--run-id', str(run)] + PROFILE_EXTENSION, 
            stdout=subprocess.PIPE
        ).stdout
    
    print("Completed cancelling runs.")
    