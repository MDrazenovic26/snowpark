# Snowpark Session Util
**A simple utility for spark and mlflow session objects**


## Setup

### Quick Install

```shell
python -m pip install snowpark_session
```

### Build from source

Clone the repository

```shell
git clone https://github.com/Broomva/snowpark_session.git
```

Install the package

``` shell
cd snowpark_session && make install
```

### Build manually

After cloning, create a virtual environment

```shell
conda create -n snowpark_session python=3.8
conda activate snowpark_session
conda install snowflake-snowpark-python pandas
```

Install the requirements

```shell
pip install -r requirements.txt
```

Run the python installation

```shell
python setup.py install
```

## Usage

The deployment requires a .env file created under local folder:

```shell
touch .env
```

It should have a schema like this:

```toml
databricks_experiment_name=''
databricks_experiment_id=''
databricks_host=''
databricks_token=''
databricks_username=''
databricks_password=''
databricks_cluster_id=''
```

```python
import snowpark_session 

# Create a Spark session
spark = DatabricksSparkSession().get_session()

# Connect to MLFLow Artifact Server
mlflow_session = DatabricksMLFlowSession().get_session()
```