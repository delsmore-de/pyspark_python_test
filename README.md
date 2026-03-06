# pyspark_python_test
A repo for practicing with pyspark in python

## General Macbook Setup:
1. Install Visual Studio Code
2. Install Homebrew: https://brew.sh/
3. Install Java 17
   - ex:
            `brew install openjdk@17`
   - .zshrc should have:
`export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"`
`export JAVA_HOME=$(/usr/libexec/java_home -v 17)`
   - Run `java -version` to verify
   - If you are runing into issues, try restarting your terminal
4. Install git
5. Install uv/venv/anaconda
   - project was setup in uv `brew install uv`
6. clone repo and create virtual env
   - ex: `uv sync`
7. Update code to add transformations
   - run locally with `uv run main.py`
   - run pytests with `uv run pytest`

## contents
/data contains both a csv and a parquet file of the iris dataset
/pipeline contains the example pipeline class as well as its transformations
/tests contains some example pytests
/utils contains methods for creating a spark session

## Senarios
   1. There is a test case currently failing that is checking that the sepallength column is rounded to the nearest whole number. 
   create a transformation that rounds this column and add it to the pipeline.
   2. No output is given.  Add flat file output
   3. When running this pipeline, a temporary instance of delta is created.  Create the transformations output as tables and export table data at the end of the pipeline run.
