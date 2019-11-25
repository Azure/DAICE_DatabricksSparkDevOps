# DevOps for a Spark Jar, Egg Jobs

**Assumptions:**
1. You have created one or more (Dev/QA and Prod) Databricks Workspaces.
1. You have generated a [Personal Access Token (PAT)](https://docs.databricks.com/dev-tools/api/latest/authentication.html#generate-a-token).
1. You have created at least one mount point called `/mnt/jars` to house the jars or egg packages.
    * In this demo case, it's expecting two other mount points
    * `/mnt/input` with a file called `/mnt/input/bank/bank.csv` pulled from [UCI Machine Learning Repo](https://archive.ics.uci.edu/ml/datasets/Bank+Marketing)
    * `/mnt/output`
1. You are familiar with either Scala or Python.

## Build Pipeline

### ScalaSpark_App_Build
In the build pipeline, you are going to create the Jar as an artifact produced by the build.

1. Create a new Build Pipeline and name it ScalaSpark_App_Build
1. Choose "Use the classic editor" for a visual development experience.
1. Select a source
    * Azure Repos Git
    * Repository: D_AI_CE_DatabricksSparkDevOps
    * Default Branch: master
1. Search for a "Maven" template and apply.
1. In the Maven task, unlink the default Maven POM file and re-link it with `Scala/SparkSimpleApp/pom.xml`
1. All other default options are okay.
1. Save & queue.  You should now have a running pipeline.

### Pyspark_App_Build
1. Create a new Build Pipeline and name it Pyspark_App_Build
1. Choose "Use the classic editor" for a visual development experience.
1. Select a source
    * Azure Repos Git
    * Repository: D_AI_CE_DatabricksSparkDevOps
    * Default Branch: master
1. Search for a "Python" template and apply.
    * Delete the disabled Flake8 task.
    * For each task, if the "Advanced > Working Directory" option is present, set it as `Python/SparkSimpleApp`.
1. Select the versions of Python you will test against by changing the Variable `python.version` to `3.6, 3.7, 3.8`.
1. Change the `pytest command line` task to `pip install .\ && pip install pytest && pytest tests --doctest-modules --junitxml=junit/test-results.xml`
   * This will install the current working directory's package (`pip install .\` with the working directory set to `Python/SparkSimpleApp`).
1. Set the `Use Python` task under Publish to `3.7`.
1. Change `Build sdist`'s script to be `python setup.py bdist_egg` and change its Display name to `Build egg`
1. Change `Publish Artifact: dist` tasks' Path to Publish to `Python/SparkSimpleApp/dist`
1. All other default options are okay.
1. Save & queue.  You should now have a running pipeline.


## Release Pipelines

The release pipeline allows you to deploy your jar or egg job to your target compute: Databricks Spark.  Create your Release pipeline by going to Pipelines > Releases > + New Release Pipeline.  Start with an Empty Job.

!['Release Pipeline Artifacts'](./docs/img/artifacts-release.png)

* Add two artifacts:
  * Build: Choose the source build pipeline, default verison of Latest and default Source Alias (`_ScalaSpark_App_Build` or `_Pypark_App_Build`).
  * Azure Repo Git: Choose the repository that your code exists in.  Name it **_code**.

* Add two stages:
  * QA
  * Prod

* Add two variable groups:
  * DevDatabricksVariables: Region and Token for Dev / QA environment
    * Add `DATABRICKS_REGION` (e.g. centralus or eastus2)
    * Add `DATABRICKS_TOKEN` and add your [Personal Access Token](https://docs.databricks.com/dev-tools/api/latest/authentication.html#generate-a-token).  Apply the Lock to make it a hidden variable.
    * Choose the Scope as Stage > QA
  * ProdDatabricksVariables: Region and Token for production environment
    * Add `DATABRICKS_REGION` (e.g. centralus or eastus2)
    * Add `DATABRICKS_TOKEN` and add your [Personal Access Token](https://docs.databricks.com/dev-tools/api/latest/authentication.html#generate-a-token).  Apply the Lock to make it a hidden variable.
    * Choose the Scope as Stage > Prod

* Add the Microsoft DevLabs' [DevOps for Azure Databricks](https://marketplace.visualstudio.com/items?itemName=riserrad.azdo-databricks) extension.  This will give us some handy tasks that sit on top of the Databricks CLI.  This will, however, force us to use a Windows Agent in the Release pipelines.
  * There's another extension by Data Thirst: [Databrick Script Deployment Task](https://marketplace.visualstudio.com/items?itemName=DataThirstLtd.databricksDeployScriptsTasks).  
  * Feel free to explore this extension as it has additional UI driven tasks for the Databricks CLI.

### Release Scala Databricks

!['Release Tasks for Scala Jar'](./docs/img/dbr-jar-release.png)

Add the following tasks to both the QA and Prod stages (Pro Tip: You can do this once in QA and then Clone the stage and rename).

1. Use Python Version 
    * Set Version Spec to 3.6
1. Configure Databricks (from Microsoft DevLabs)
   * Set Workspace URL to `https://$(DATABRICKS_REGION).azuredatabricks.net`
   * Set Access Token to `$(DATABRICKS_TOKEN)`
   * This creates a Databricks configuration profile of `AZDO`.  We pass this to the deployment.py file.
1. Databricks file to DBFS
   * Set Azure Region to `$(DATABRICKS_REGION)`
   * Set Local Root Folder to `$(System.DefaultWorkingDirectory)/_ScalaSpark_App_Build/drop/Scala/SparkSimpleApp/target`
   * Set File Pattern to `*.jar`
   * Set Target folder in DBFS to `/mnt/jars/`
   * Set Security 
     * Authentication Method: Bearer Token
     * Databricks Bearer token: `$(DATABRICKS_TOKEN)`
1. Python Script
   * Script Source: File Path
   * Script Path: `$(System.DefaultWorkingDirectory)/_code/deployment.py`
  
  ```
  jar $(System.DefaultWorkingDirectory)/_ScalaSpark_App_Build/drop/Scala/SparkSimpleApp/target dbfs:/mnt/jars $(System.DefaultWorkingDirectory)/_code/Scala/SparkSimpleApp/job.json --main-class com.microsoft.spark.example.SparkSimpleApp --parameters "/mnt/input/bank/bank.csv" "/mnt/output/SparkSimpleAppPY/test.csv" --profile AZDO
  ```

You now have a working release pipeline!  Save and execute the Release!

### Release Egg Databricks

!['Release Tasks for Python Egg'](./docs/img/dbr-egg-release.png)

Add the following tasks to both the QA and Prod stages (Pro Tip: You can do this once in QA and then Clone the stage and rename).

1. Use Python Version 
    * Set Version Spec to 3.6
1. Configure Databricks (from Microsoft DevLabs)
    * Set Workspace URL to `https://$(DATABRICKS_REGION).azuredatabricks.net`
    * Set Access Token to `$(DATABRICKS_TOKEN)`
    * This creates a Databricks configuration profile of `AZDO`.  We pass this to the deployment.py file.
1. Databricks file to DBFS
    * Set Azure Region to `$(DATABRICKS_REGION)`
    * Set Local Root Folder to `$(System.DefaultWorkingDirectory)/_Pyspark_App_Build/dist`
    * Set File Pattern to `*.jar`
    * Set Target folder in DBFS to `/mnt/jars/`
    * Set Security 
      * Authentication Method: Bearer Token
      * Databricks Bearer token: `$(DATABRICKS_TOKEN)`
1. Databricks file to DBFS
    * Set the settings the same as above with the following exceptions.
    * Set Local Root Folder to `$(System.DefaultWorkingDirectory)/_code/Python/SparkSimpleApp`
    * Set File Pattern to `main.py`
1. Python Script
    * Script Source: File Path
    * Script Path: `$(System.DefaultWorkingDirectory)/_code/deployment.py`
  
  ```
  egg $(System.DefaultWorkingDirectory)/_Pyspark_App_Build/dist/ dbfs:/mnt/jars $(System.DefaultWorkingDirectory)/_code/Python/SparkSimpleApp/job.json --python-file "dbfs:/mnt/jars/main.py" --parameters "/mnt/input/bank/bank.csv" "/mnt/output/SparkSimpleAppPY/test.csv" --profile AZDO
  ```

You now have a working release pipeline!  Save and execute the Release!

# deployment.py

The deployment.py file helps abstract the calls to the Databricks CLI and enables you to replace text in the job's json definition.

The help file below describes the usage.

```
usage: deployment.py [-h] [--python-file PYTHON_FILE]
                     [--main-class MAIN_CLASS]
                     [--parameters [PARAMETERS [PARAMETERS ...]]]
                     [--profile PROFILE]
                     [--update-if-exists UPDATE_IF_EXISTS UPDATE_IF_EXISTS]  
                     {jar,egg} library_path cloud_path job_json

Deploy a set of jar or egg files as a Spark application

positional arguments:
  {jar,egg}             Valid options are jar or egg
  library_path          The library or folder containing libraries to include
  cloud_path            The path in the cloud (e.g. DBFS, WASB) that the
                        library is located
  job_json              The path to the job definition (only applicable to
                        Databricks)

optional arguments:
  -h, --help            show this help message and exit
  --python-file PYTHON_FILE
                        The python file that runs the python application
  --main-class MAIN_CLASS
                        The main class of your scala jar application
  --parameters [PARAMETERS [PARAMETERS ...]]
                        List of parameters that get passed directly to the
                        spark jar / python task
  --profile PROFILE     Profile name to be passed to the databricks CLI
  --update-if-exists UPDATE_IF_EXISTS UPDATE_IF_EXISTS
                        Looks for a job_id or name (useful only for Databricks
                        deployments)
```