# Minimal Ed-Fi API CLI

This tool allows you to explore the Ed-Fi API at an endpoint via cli commands.
This tool only operates the Ed-Fi api from the perspective of a consumer.

NOTE: At the moment no data is pushed to the Ed-Fi ODS with this tool

## Why

Exploring Ed-Fi endpoints via curl/postman is a hassle. For instance, getting
page 10 of the dataset uses hard to remember patterns.  It just makes sense to
have a tool to quickly inspect Ed-FI AI data.

## Prerequisites

Pip install

```bash
pip3 install -r requirements.txt
```

## Configuration

Copy config.template to config.toml and alter to taste.  Each customer needs
their own section in the config.toml (i.e., you might name them
config.Customer1234, config.CustomerABCD)

The config file is in TOML format and has at a minimum 2 sections.  The first
section is a "general" section which applies to all environments.  The general
section takes the form of:

```none
[general]
profile_logging=true
logging_format="json"
max_workers=4
async_requests=true
```

where:

* profile_logging - set to "true" to enable per call profile logging (logs
    to profile.log)
* logging_format - format of log entries - set to "json" for writing each line
    as json, otherwise the writing of the log will be standard python log
    format
* async_requests - set to "true" to create threads to handled multiple
    aysnchonous requests otherwise set to "false" for serialized requests
* max_workers - when async_requests is set to "true" max_workers defines the
    the number of threads to create.  If max_workers is not specified, the
    max_workers will be set to the number of processors in the systems time
    5 - see [Python Thread Pool Executor](https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor)

One or more customer sections describes the connection information for a
customer. Customer connection information takes the form of:

```none
[<customer name>]
edfi_client_id="<edfi client id>"
edfi_client_secret="<edfi client secret>"
edfi_base_url="<full url to the ODS API endpoint>"
verify_ssl=<false|true> # for SSL verification>
api_ver="v<edfi version>"
default_year=2018
```

where:

* customer name - the name of the customer and typically the target
    operating environment (production, staging) - e.g. prod_cust123
* edfi client id - the client id used to authenticate to the edfi api
* edfi client secret - the client secret used to authenticate to the edfi api
* edif_base_url - the full url to the ODS api endpopint - e.g.:
    https://api_production.example.org/ODS
* verify_ssl - for self-signed certs on test ODS instances, you can set this
    to "false", otherwise omit or set to "true"
* api_ver - the version of the api - e.g., 3.1
* default_year - use the default year - in EdFi v2.x, the year is part of the
    request and must be specified

## How

To see usage of the tool run:

```bash
python3 edfi.py help
```

A script to perform full extraction can be found at test.sh

## Legal Information

Copyright (c) 2021 Ed-Fi Alliance, LLC and contributors.

Licensed under the [Apache License, Version 2.0](LICENSE) (the "License").

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

See [NOTICES](NOTICES.md) for additional copyright and license notifications.
