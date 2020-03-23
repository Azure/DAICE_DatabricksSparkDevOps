import argparse
import json
import os
from pathlib import posixpath
import re
import subprocess

def parse_jobs_list(cli_output):
    """
    Parse Databricks CLI output of `databricks jobs list` to return
    a list of job ids and their names.
    """
    jobs = cli_output.decode('utf-8').replace('\r\n','\n').split('\n')
    output = {}
    for job in jobs:
        matches = re.search('(\d+) +(.+)', job)
        if matches:
            output[matches.group(1)] = matches.group(2)
    return output


if __name__ == "__main__":
    """
    Creates a Spark application deployment by wrapping the Databricks CLI
    and modifying the related job json file.

    """
    parser = argparse.ArgumentParser(
        description="Deploy a set of jar or egg files as a Spark application"
    )
    parser.add_argument('objective',
        default="jar",
        choices=["jar","egg", "notebook"],
        help="Valid options are jar, egg, or notebook")
    parser.add_argument('library_path',
        help="The library or folder containing libraries to include. Use na for no libraries.")
    parser.add_argument('cloud_path',
        help="The path in the cloud (e.g. DBFS, WASB) that the library is located. Use na for no libraries.")
    parser.add_argument('job_json',
        help="The path to the job definition (only applicable to Databricks)")
    parser.add_argument('--python-file',
        help="(egg option) The python file that runs the python application")
    parser.add_argument('--main-class',
        help="(jar option) The main class of your scala jar application")
    parser.add_argument('--notebook-path',
        help="(notebook option)The path to your notebook in your databricks workspace")
    parser.add_argument('--profile',
        default=None,
        help="Profile name to be passed to the databricks CLI"
    )
    parser.add_argument('--update-if-exists',
        nargs=2,
        default=None,
        help="Looks for a job_id or name (useful only for Databricks deployments)"
    )
    parser.add_argument('--parameters',
        nargs=argparse.REMAINDER,
        default = [],
        help="List of parameters that get passed directly to the spark jar / python task.  This must be the last parameter."
    )
    args = parser.parse_args()

    map_objective_to_task = {
        "jar": "spark_jar_task",
        "egg": "spark_python_task",
        "notebook": "notebook_task"
    }

    with open(args.job_json, 'r') as jobfp:
        job_def = json.load(jobfp)
    
    # If the library path attribute is na then skip adding libraries
    # Is it one or many objects to install as libraries?
    if args.library_path.strip().lower() != "na":
        if os.path.isdir(args.library_path):
            # Directory path specified, grab all files of type args.objective
            # TODO: Decide if this should be recursive or not?
            all_packages = [
                p for p in os.listdir(args.library_path) 
                    if os.path.splitext(p)[1] == '.' + args.objective
            ]
        else:
            all_packages = [args.library_path]
        
        # Get the library's name and it's destination folder
        # Replace the job.json's content
        job_def["libraries"] = [
            {args.objective: posixpath.join(args.cloud_path, package)} for package in all_packages
        ]

    # Get the task type based on the passed in objective
    objective_task_name = map_objective_to_task[args.objective]
    if objective_task_name == "spark_python_task":
        # You need a python_file to run the app
        job_def[objective_task_name] = {
            "python_file": args.python_file
        }
    elif objective_task_name == "spark_jar_task":
        # You need a main_class_name to run the app
        job_def[objective_task_name] = {
            "main_class_name": args.main_class
        }
    else:
        # Assuming notebook task
        job_def[objective_task_name] = {
            "notebook_path": args.notebook_path
        }
        # Adding in parameters if they are available
        if args.parameters:
            # Assuming --parameters key1 value1 key2 value2
            # If it's not an even set of pairs
            if len(args.parameters) % 2 != 0:
                raise IndexError("Parameters passed into a notebook task must be an even number of attributes as it assumes key, value pairs")
            
            pair_indexes = [x for x in range(len(args.parameters)) if x %2 == 0]
            
            job_def[objective_task_name].update(
                {"base_parameters": {args.parameters[x]:args.parameters[x+1] for x in pair_indexes }}
            )
    
    # Back to the main flow
    # Parameters is an attribute across egg and jar tasks
    if args.parameters and objective_task_name != "notebook_task":
        job_def[objective_task_name].update(
            {"parameters":args.parameters}
        )
    
    PROFILE_EXTENSION = ['--profile', args.profile] if args.profile else []

    # Look to see if the job exists already (title or jar or id)
    EXISTING_JOB = None
    CLI_VERB = 'create'
    if args.update_if_exists:
        cli_output = subprocess.run(
            ['databricks', 'jobs', 'list'] + PROFILE_EXTENSION, 
            stdout=subprocess.PIPE
        ).stdout

        jobs_on_databricks = parse_jobs_list(cli_output)

        if args.update_if_exists[0] == "job_id":
            if args.update_if_exists[1] in jobs_on_databricks.keys():
                EXISTING_JOB = args.update_if_exists[1]
        elif args.update_if_exists[0] == "name":
            if args.update_if_exists[1] in jobs_on_databricks.values():
                candidate_jobs = list(filter(
                    lambda tup: tup[1] == args.update_if_exists[1],
                    jobs_on_databricks.items()
                ))
                # Results from Databricks output are not sorted by job_id
                # Assuming you want to update the most recent job_id
                candidate_jobs = sorted(
                    candidate_jobs, 
                    key=lambda x: int(x[0]), 
                    reverse=True
                )

                EXISTING_JOB = candidate_jobs[0][0]
    
    if EXISTING_JOB:
        print("Job {}: {} exists.  Updating specifications".format(
            EXISTING_JOB, jobs_on_databricks[EXISTING_JOB]
        ))
        CLI_VERB = 'reset'

    else:
        print('Deploying a new job')
    
    # Create the job on databricks or edit existing
    deployment_command = ['databricks', 'jobs', CLI_VERB, '--json', json.dumps(job_def)]
    if CLI_VERB == 'reset':
        deployment_command.extend( ['--job-id', EXISTING_JOB])
    
    # Add any profile options to the command if it's present in args.profile
    deployment_command.extend( PROFILE_EXTENSION )
    
    print('Attempting to run:\n{}'.format(' '.join(deployment_command)))
    output_job_id = None
    
    call_results_json = subprocess.run(deployment_command, stdout=subprocess.PIPE).stdout

    # If we are creating a new job, databricks cli returns a json output
    # Parse the output and store the job_id
    if CLI_VERB == 'create':
        try:
            call_results = json.loads(
                call_results_json.decode('utf-8').replace('\r\n','\n')
            )
            output_job_id = call_results.get("job_id")

        except Exception as e:
            print(e)
            print("Original JSON: {}".format(call_results_json) )
            raise e

    # Select the job id from either the existing job or created job
    resulting_job_id = EXISTING_JOB or output_job_id
    print("Succeeded in performing {} with job_id {}".format(
        CLI_VERB, resulting_job_id
    ))
    if call_results_json.decode('utf-8').lower().startswith("error"):
        raise Exception("The Databricks Job deployment failed with error: {}".format(call_results_json.decode('utf-8')))
    else:    
      # This prints a specific format for Azure DevOps to pick up output as a variable
      print("##vso[task.setvariable variable=DBR_JOB_ID]{}".format(resulting_job_id))
