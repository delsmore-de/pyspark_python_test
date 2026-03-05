# pyspark_python_test
A repo for practicing pyspark in python

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

    
