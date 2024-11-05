# Window Manager
Creates and manages the event windows (VEKG list)


# Events Listened
 - [QUERY_CREATED](https://github.com/Gnosis-MEP/Gnosis-Docs/blob/main/EventTypes.md#QUERY_CREATED)
 - [QUERY_REMOVED](https://github.com/Gnosis-MEP/Gnosis-Docs/blob/main/EventTypes.md#QUERY_REMOVED)
 - [VEKG](https://github.com/Gnosis-MEP/Gnosis-Docs/blob/main/EventTypes.md#VEKG)

# Events Published
 - [VEKG_STREAM](https://github.com/Gnosis-MEP/Gnosis-Docs/blob/main/EventTypes.md#VEKG_STREAM)


# Installation

## Configure .env
Copy the `example.env` file to `.env`, and inside it replace the variables with the values you need.

## Installing Dependencies

### Using pipenv
Run `$ pipenv shell` to create a python virtualenv and load the .env into the environment variables in the shell.

Then run: `$ pipenv install` to install all packages, or `$ pipenv install -d` to also install the packages that help during development, eg: ipython.
This runs the installation using **pip** under the hood, but also handle the cross dependency issues between packages and checks the packages MD5s for security mesure.


### Using pip
To install from the `requirements.txt` file, run the following command:
```
$ pip install -r requirements.txt
```

# Running
Enter project python environment (virtualenv or conda environment)

**ps**: It's required to have the .env variables loaded into the shell so that the project can run properly. An easy way of doing this is using `pipenv shell` to start the python environment with the `.env` file loaded or using the `source load_env.sh` command inside your preferable python environment (eg: conda).

Then, run the service with:
```
$ ./window_manager/run.py
```

# Testing
Run the script `run_tests.sh`, it will run all tests defined in the **tests** directory.

Also, there's a python script at `./window_manager/send_msgs_test.py` to do some simple manual testing, by sending msgs to the service stream key.


# Docker
## Build
Build the docker image using: `docker-compose build`

**ps**: It's required to have the .env variables loaded into the shell so that the container can build properly. An easy way of doing this is using `pipenv shell` to start the python environment with the `.env` file loaded or using the `source load_env.sh` command inside your preferable python environment (eg: conda).

## Run
Use `docker-compose run --rm service` to run the docker image

## Benchmark Tests
To run the benchmark tests one needs to manually start the Benchmark stage in the CI pipeline (Gitlab), it shoud be enabled after the tests stage is done. Only by passing the benchmark tests shoud the image be tagged with 'latest', to show that it is a stable docker image.
