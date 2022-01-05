# Unzip service

## Synopsis

Subscribes to a Pulsar topic to get notified of incoming zip files which get extracted to a specified folder. An event is published to Pulsar to make it known the extraction was succesful.

## Prerequisites

- Git
- Docker (optional)
- Python 3.6+ (**Python 3.9 and up not yet supported**)
- Access to the [meemoo PyPi](http://do-prd-mvn-01.do.viaa.be:8081)

## Usage

1. Clone this repository with:

   `$ git clone https://github.com/viaacode/unzip-service.git`

2. Change into the new directory.

3. Set the needed config:

    Included in this repository is a `config.yml.example` file. 
    All values in the config have to be set in order for the application to function correctly.
    You can use `!ENV ${EXAMPLE}` as a config value to make the application get the `EXAMPLE` environment variable.

### Running locally

**Note**: As per the aforementioned requirements, this is a Python3
application. Check your Python version with `python --version`. You may want to
substitute the `python` command below with `python3` if your default Python version
is < 3. In that case, you probably also want to use `pip3` command.

1. Start by creating a virtual environment:

    `$ python -m venv env`

2. Activate the virtual environment:

    `$ source env/bin/activate`

3. Install the external modules:

    ```
    $ pip install -r requirements.txt \
        --extra-index-url http://do-prd-mvn-01.do.viaa.be:8081/repository/pypi-all/simple \
        --trusted-host do-prd-mvn-01.do.viaa.be && \
      pip install -r requirements-test.txt
    ```

4. Run the tests with:

    `$ pytest -v --cov=./app`

5. Run the application:

    `$ python main.py`

### Running using Docker

1. Build the container:

   `$ docker build -t unzip-service .`

2. Run the container (with specified `.env` file):

   `$ docker run --env-file .env unzip-service:latest`