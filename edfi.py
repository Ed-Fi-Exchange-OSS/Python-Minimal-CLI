# #############################################################################
# edfi cli tool
# 
# consumer focused cli tool for exploring the edfi api space
# tested against EdFi ODS 2.x amd 3.1
# 
# run python3 edfi.py to see usage
#
# Must create config.toml with credentials to one or more endpoints - see
#   config.template for an example, copy and alter to environment
#
# NOTE: this does not utilize the "change set" feature found in EdFi v3.x
# releases (at this time)
#
# #############################################################################
import inspect
import json
import logging
import os
from queue import Queue
import sys
from threading import Thread
import time

import click
import requests
from requests.auth import HTTPBasicAuth
import toml

__version__ = "0.0.1"

logging.captureWarnings(True)

class Config(object):
    """
    Manages config files
    """
    config = {}

    def __init__(self):
        """ init """
        self.load_config()

    def load_config(self):
        """ loads the config """
        try:
            self.config = toml.load(open("config.toml"))
        except Exception as exp:
            echo("Could not load config - %s" % exp, FAIL)
            sys.exit(1)

class EdFi(object):
    """ class to do edfi api stuff """
    cfg = {}
    baseurl = None
    api_ver = "v2.0"
    year = None
    headers = {"Content-Type": "application/json"}
    profilelogger = None
    verify_ssl = True

    def __init__(self, year:str, customer_id:str):
        """ inity stuff """
        self.customer_id = customer_id
        self.year = year
        self.cfg = Config().config

        if customer_id not in self.cfg:
            echo("Customer '%s' not found in config" % customer_id, FAIL)
            sys.exit(1)
        for f in ['edfi_base_url', 'edfi_client_secret', 'edfi_client_id']:
            if f not in self.cfg[customer_id]:
                echo("%s not found in config for customer '%s'" % (f,  customer_id), FAIL)
                sys.exit(1)
        if 'api_ver' in self.cfg[customer_id]:
            self.api_ver = self.cfg[customer_id]['api_ver']

        self.baseurl = self.cfg[customer_id]["edfi_base_url"]
        if 'verify_ssl' in self.cfg[customer_id]:
            try:
                self.verify_ssl = bool(self.cfg[customer_id]['verify_ssl'])
            except:
                pass

        while self.baseurl.endswith("/"):
            self.baseurl = self.baseurl[0:-1]
        # from: https://techdocs.ed-fi.org/display/ODSAPI23/Authentication
        # authorize
        #   curl https://api.ed-fi.org/api/oauth/authorize -d "Client_id=<clientid>&Response_type=code"
        #   ^^^ returns: {"code": "<code>"}
        # get token
        #   curl https://api.ed-fi.org/api/oauth/token -H "Content-Type: application/json" -d "{'Client_id':'<clientid>','Client_secret':'<client secret>','Code':'R3PLAC3_W1TH_AUTH_COD3','Grant_type':'authorization_code'}"
        #   ^^^ returns: {"access_token": "<token<", "expires_in": 22199, "token_type": "bearer"}

        self.get_auth_token()

        # setup profiler if needed
        if "general" in self.cfg and "profile_logging" in self.cfg['general'] and self.cfg['general']['profile_logging']:
            self.profilelogger = logging.getLogger('profile')
            self.profilelogger.setLevel(logging.INFO)
            logfilename = 'profile.log'
            formatter = logging.Formatter("%(asctime)s dur:%(duration)s - %(message)s") # default formatter
            if 'logging_format' in self.cfg['general'] and self.cfg['general']['logging_format'] == "json":
                # brute force json logging format - but it works
                formatter = logging.Formatter('{"datetime":"%(asctime)s","duration":%(duration)s,"message":"%(message)s"}')
            fh = logging.FileHandler(logfilename)
            fh.setFormatter(formatter)
            self.profilelogger.addHandler(fh)
            echo("Profile logging enabled - writing to " + logfilename, INFO)

    def get_auth_token(self):
        # get token based on version
        if self.api_ver.startswith("v3."):
            ## v3 token process
            url = "{}/api/oauth/token".format(self.baseurl)
            auth = HTTPBasicAuth(self.cfg[self.customer_id]["edfi_client_id"],self.cfg[self.customer_id]["edfi_client_secret"])
            try:
                res = requests.post(url, json={"grant_type":"client_credentials"}, verify=False, auth=auth)
                self.headers.update({"Authorization":  "Bearer {}".format(res.json()['access_token'])})
            except Exception as exp:
                echo("Could not authenticate to 3.x api instance %s - %s" % (exp, res.content), FAIL)
                sys.exit(1)

        else:
            # default to 2.x
            url = "{}/api/oauth/authorize".format(self.baseurl)
            data = {"Client_id": self.cfg[self.customer_id]["edfi_client_id"], "Response_type": "code"}
            auth_code = None
            try:
                res = requests.post(url, json=data, verify=self.verify_ssl)
                if res.status_code > 399:
                    raise Exception("HTTP error - {} on authorize to {} - {}".format(res.status_code, url, res.content))
                if "error" in res.json() and res.json()['error']:
                    raise Exception("EDFI Authorize error on {} - {}".format(url, res.json()['error']))
                auth_code = res.json()['code']
            except Exception as exp:
                echo(exp, FAIL)
                sys.exit(1)
        
            url = "{}/oauth/token".format(self.baseurl)
            data = {
                "Client_id": self.cfg[self.customer_id]["edfi_client_id"],
                "Client_secret": self.cfg[self.customer_id]["edfi_client_secret"],
                "Code": auth_code,
                'Grant_type':'authorization_code'
            }
            try:
                res = requests.post(url, json=data, headers={"Content-Type": "application/json"}, verify=self.verify_ssl)
                if res.status_code > 399:
                    raise Exception("HTTP error - {} on authenticate to {} - {}".format(res.status_code, url, res.content))
                self.headers.update({"Authorization":  "Bearer {}".format(res.json()['access_token'])})
            except Exception as exp:
                echo(exp, FAIL)
                sys.exit(1)

        
  
    def profile(self, description, duration):
        """ logs profiling information """
        if not self.profilelogger:
            return
        self.profilelogger.info(description, extra={"duration":duration})

    def __build_url(self, endpoint):
        """ url builder """
        if self.api_ver.startswith("v2."):
            return "{base}/api/{ver}/{year}/{endpoint}".format(
                base=self.baseurl,
                ver=self.api_ver,
                year=self.year,
                endpoint=endpoint
            )
        if self.api_ver.startswith("v3."):
            _endpoint = endpoint
            if not endpoint.endswith("s"):
                _endpoint += "s"
            #curl -X GET "https://edfi-mich-test-web.southcentralus.cloudapp.azure.com:443/v3.1.0/api/data/v3/ed-fi/students?offset=0&limit=25&totalCount=false" -H "accept: application/json"
            #             https://edfi-mich-test-web.southcentralus.cloudapp.azure.com/v3.1.0/api/api/data/v3/ed-fi/academicWeeks
            return "{base}/api/data/v3/ed-fi/{endpoint}".format(
                base=self.baseurl,
                ver=self.api_ver,
                year=self.year,
                endpoint=_endpoint
            )
        raise Exception("Could not determine version in order to build url")


    def worker_get(self, url, offset, limit):
        """ performs the get """
        _url = "{}?offset={}&limit={}".format(url, offset, limit)
        retries = 0
        data = []
        while retries < 5:
            retries += 1
            try:
                res = requests.get(_url, headers=self.headers, verify=self.verify_ssl)
                if res.status_code == 401:
                    self.get_auth_token()
                    time.sleep(1)
                    continue

                if res.status_code > 299:
                    echo("error on url: {}".format(url), FAIL)
                    raise Exception("HTTP error - {} on get to {} - {}".format(res.status_code, _url, res.content))
                self.profile("GET "+_url, res.elapsed.total_seconds())
                data = res.json()
                break
            except Exception as exp:
                msg = "Could not get data from %s - %s" % (_url, res.content)
                print(msg)
                raise Exception(msg)

        return data

    def queue_worker(self):
        """ worker of queues """
        while True:
            payload = self.q.get()
            if not payload:
                break
            invalid = False
            for f in ['workers', 'url', 'limit', 'offset']:
                if f not in payload:
                    invalid = True
            if invalid:
                break

            try:
                res = self.worker_get(payload['url'], payload['offset'], payload['limit'])
                if not res:
                    raise Exception("No more data")
                self.__data.extend(res)  # this is thread safe
                # submit a new task
                payload['offset'] = payload['offset'] + (payload['workers'] * payload['limit']) # update with new page
                time.sleep(0.5)
                self.q.put(payload)
            except Exception as exp:
                # no data, must be end
                pass
            self.q.task_done()


    def get_parallel(self, url,limit):
        """ gets via parallel operations """

        # for each page, get data
        max_workers = None # if none, use 5x processors of host as count - https://docs.python.org/3/library/concurrent.futures.html
        if 'general' in self.cfg and 'max_workers' in self.cfg['general']:
            try:
                max_workers = max(1,int(self.cfg['general']['max_workers']))
            except:
                pass

        self.q = Queue()
        threads = []
        self.__data = []
        for i in range(max_workers):
            t = Thread(target=self.queue_worker)
            t.start()
            threads.append(t)

        for i in range(max_workers): #start with max_wokers number of workers
            self.q.put(dict(url=url, limit=limit, workers=max_workers, offset=i*limit))

        self.q.join()

        for i in range(max_workers):
            self.q.put(None)
        for t in threads:
            t.join()

        return self.__data

    def get_serial(self, url, page=0, limit=100):
        """ gets data serially """
        data= []
        if page == -1: # get all
            qs = {"limit": limit, "offset":0}
        else:
            qs = {"limit": limit, "offset": page*limit}
        while True:
            _data = self.worker_get(url, qs['offset'], qs['limit'])
            if not _data:
                break
            if isinstance(_data, list):
                data.extend(_data)
            else:
                data.append(_data)
            qs['offset'] = qs['offset'] + qs['limit']
            if page > -1:
                break  # we are getting a specific page, so just break and move on
        return data

    def get(self, endpoint, page=0, limit=100):
        """ get 'factory' """
        url = self.__build_url(endpoint)
        if page<1 and 'general' in self.cfg and 'async_requests' in self.cfg['general'] and self.cfg['general']['async_requests']:
            return self.get_parallel(url, limit)
        return self.get_serial(url, page, limit)

    def build_properties_2x(self, models, prop_name, prop):
        """ builds the properties """
        props = {}

        if prop['type'] in ["integer", "string", "boolean", "date-time", "number"]:
            # simple type
            props[prop_name] = prop['type'].replace("-", "")
        elif prop['type'] == "array":
            props[prop_name] = {}
            for subprop_name, subprop in models[prop['items']['$ref']]['properties'].items():
                props[prop_name].update(self.build_properties_2x(models, subprop_name, subprop))
        else:
            if prop['type'] not in models:
                raise Exception("Could not find model %s to build property %s" % (prop['type'], prop_name))
            for ref_key, ref_prop in models[prop['type']]['properties'].items():
                base_key = "{}_{}".format(prop_name, ref_key)
                if ref_prop['type'] in ['link']:
                    props["{}_href".format(base_key)] = "string"
                    props["{}_rel".format(base_key)] = "string"
                else:
                    props[base_key] = ref_prop['type']
        return props

    def structure(self, endpoint):
        """ returns the structure of an endpoint """
        if self.api_ver.startswith("v2."):
            return self._structure_2x(endpoint)
        elif self.api_ver.startswith("v3."):
            return self._structure_3x(endpoint)
        return None

    def _structure_3x(self, endpoint):
        """ fetches the structure of endpoints for 3.x"""
        data = {}
        data.update(self._get_endpoint_data_3x())

        _endpoint = "edFi_" + endpoint

        if _endpoint not in data['definitions']:
            raise Exception("No endpoint '%s' was found in endpoint data" % endpoint)
        if 'properties' not in data['definitions'][_endpoint]:
            raise Exception("No properties were found for endpoint '%s'" % endpoint)
        properties = {}
        for prop_name, prop in data['definitions'][_endpoint]['properties'].items():
            if 'type' not in prop:
                continue
            properties.update(self.build_properties_3x(data['definitions'], prop_name, prop))

        return properties

    def build_properties_3x(self, definitions, prop_name, prop):
        """ builds properties for ver 3x """
        props = {}

        if prop['type'] in ["integer", "string", "boolean", "date-time", "number"]:
            # simple type
            props[prop_name] = prop['type'].replace("-", "")
        elif prop['type'] == "array":
            props[prop_name] = {}
            for subprop_name, subprop in definitions[os.path.basename(prop['items']['$ref'])]['properties'].items():
                props[prop_name].update(self.build_properties_3x(definitions, subprop_name, subprop))
        else:
            if prop['type'] not in definitions:
                raise Exception("Could not find definition %s to build property %s" % (prop['type'], prop_name))
            for ref_key, ref_prop in definitions[prop['type']]['properties'].items():
                base_key = "{}_{}".format(prop_name, ref_key)
                if ref_prop['type'] in ['link']:
                    props["{}_href".format(base_key)] = "string"
                    props["{}_rel".format(base_key)] = "string"
                else:
                    props[base_key] = ref_prop['type']
        return props



    def _structure_2x(self, endpoint):
        """ fetches the structure of endpoints for 2.x"""
        url = "{}/metadata/resources/api-docs/{}".format(self.baseurl, endpoint)
        data = self.get_serial(url)
        if data:
            data = data[0]
        if 'apis' not in data:
            raise Exception("No 'apis' found in structure data for %s" % endpoint)
        model = None
        for target in [x for x in data['apis'] if x['path'] == "/{}".format(endpoint)]:
            for operation in [y for y in target['operations'] if y['nickname'].lower() == "get{}All".format(endpoint).lower()]:
                if 'items' in operation and "$ref" in operation['items']:
                    model = operation['items']['$ref']
                    break
            if model:
                break

        if not model:
            raise Exception("Could not find model for endpoint %s" % endpoint)

        if 'models' not in data:
            raise Exception("No 'models' found in structure data for %s" % endpoint)

        if model not in data['models']:
            raise Exception("Could not find model %s in structure model data" % model)

        if "properties" not in data['models'][model]:
            raise Exception("Could not find properties for model %s in structure model data" % model)

        properties = {}

        for prop_name, prop in data['models'][model]['properties'].items():
            if 'type' not in prop:
                raise Exception("Not type found in property %s" % prop)
            properties.update(self.build_properties_2x(data['models'], prop_name, prop))

        return properties
        
    def get_record(self, endpoint, record_id):
        """
        Gets a particular record from an endpoint
        """
        url = "{}/{}".format(self.__build_url(endpoint), record_id)
        return self.get_serial(url)

    def get_endpoints_2x(self):
        """ gets endpoints for api ver 2x """
        url = "{}/metadata/resources/api-docs".format(self.baseurl)
        res = self.get_serial(url)
        if res:
            res = res[0]
        if res and "apis" in res:
            return [x["path"][1:] for x in res['apis']]
        return []

    def _get_endpoint_data_3x(self):
        """ gets endpoints for api ver 3x """
        url = "{}/api/metadata/data/v3/resources/swagger.json".format(self.baseurl)
        data = self.get_serial(url)
        if data:
            data = data[0]
        if not data:
            raise Exception("No data found for endpoints")
        if 'paths' not in data:
            raise Exception("Invalid endpoint data - no key 'paths' was found")
        return data

    def get_endpoints_3x(self):
        data = self._get_endpoint_data_3x()
        return sorted(list(set([x.split("/")[2] for x in data['paths'].keys() if len(x.split("/"))>2])))

    def get_endpoints(self):
        """ gets the endpoints """
        if self.api_ver.startswith("v2."):
            return self.get_endpoints_2x()
        elif self.api_ver.startswith("v3."):
            return self.get_endpoints_3x()
        return []

    def get_count(self, endpoint=None, url=None):
        """ use binary search to find end of records """
        # first find upper bounds
        if endpoint:
            url = self.__build_url(endpoint)
        n = 100
        while True:
            if self.get_serial(url, page=n, limit=1):
                n = n * 10
                continue
            break

        # now binary search in scope
        q = int(n/2)
        d = int(max(1, q/2))

        while True:
            if q > n:
                raise Exception("Cannot search past top bound")
            d = int(max(1, d/2))
            recs = self.get_serial(url, page=q, limit=2)
            if recs:
                # found records, if there is only one, then we have reached the end
                if len(recs) == 1:
                    # we have reached the end
                    n = q
                    break
                q += d
            else:
                # need to search "down"s
                q -= d
            if q < 0:
                break
        return n
        
FAIL = "red"
PASS = "green"
INFO = "blue"
def echo(msg, mode):
    """ very thin wrapper around click echo """
    click.echo(click.style(msg, fg=mode))

# ####
# CLI commands
# ####

@click.group()
def cli():
    """ does edfi api stuff """
    pass

@cli.command()
@click.argument("endpoint")
@click.argument("customerid")
@click.argument("year")
@click.option("--output", default=None, type=click.File('w'))
@click.option("--page", type=int, default=0, help="Get page by number (limit of 50), -1 for all")
@click.option("--limit", type=int, default=50, help="Number of records per page")
def get(endpoint, customerid, year, output, page, limit):
    """ gets the data from an endpoint """
    start = time.time()
    data = None
    edfi = EdFi(year=year, customer_id=customerid)
    try:
        data = edfi.get(endpoint, page, limit)
    except Exception as exp:
        echo("Could not get data for %s - %s" % (endpoint, exp), FAIL)
        sys.exit(1)
    if not data:
        echo("No data returned for endpoint %s" % endpoint, INFO)
        sys.exit(1)
    if output:
        json.dump(data, output, indent=4)
        echo("Wrote %s records from %s to %s" %(len(data), endpoint, output.name), PASS)
    else:
        echo(json.dumps(data, indent=4), PASS)
    edfi.profile("get %s (count: %d)" % (endpoint, len(data)), time.time()-start)

@cli.command()
@click.argument("endpoint")
@click.argument("customerid")
@click.argument("year")
def structure(endpoint, customerid, year):
    """ gets the structure from an endpoint """
    edfi = EdFi(year=year, customer_id=customerid)
    try:
        echo(json.dumps(edfi.structure(endpoint), indent=4), PASS)
    except Exception as exp:
        echo("Could not get structure for %s - %s" % (endpoint, exp), FAIL)

@cli.command()
@click.argument("endpoint")
@click.argument("customerid")
@click.argument("year")
def count(endpoint, customerid, year):
    """ counts the return data from an endpoint """
    edfi = EdFi(year=year, customer_id=customerid)

    try:
        echo("{} - {}".format(endpoint, edfi.get_count(endpoint=endpoint)), PASS)
    except Exception as exp:
        echo("{} - error - {}".format(endpoint, exp), FAIL)

@cli.command()
@click.argument("endpoint")
@click.argument("record_id")
@click.argument("customerid")
@click.argument("year")
def getrecord(endpoint, record_id, customerid, year):
    """
    Gets a record from an endpoint by record id
    @endpoint : the endpoint
    @record_id : the record id
    @returns : dictionary for the record
    """
    edfi = EdFi(year=year, customer_id=customerid)

    try:
        record = edfi.get_record(endpoint, record_id)
        if record:
            echo(json.dumps(record, indent=4), PASS)
        else:
            echo("No data returned for %s from %s" % (record_id, endpoint), FAIL)
    except Exception as exp:
        echo("Error trying to retrieve %s from %s - %s" % (record_id, endpoint, exp), FAIL)
    
@cli.command()
@click.argument("customerid")
@click.argument("year")
def getendpoints(customerid, year):
    """ gets the endpoints for an edfi datasource """
    edfi = EdFi(year=year, customer_id=customerid)

    endpoints = edfi.get_endpoints()
    if not endpoints:
        echo("No endpoints returned", FAIL)
        return 1
    echo(json.dumps(endpoints, indent=4), PASS)

@cli.command()
@click.argument("customerid")
@click.argument("year")
def checkendpoints(customerid, year):
    """ checks endpoints for at least one record of data - if data is found, returnes true for that endpoint, else returns false"""
    stats = {}
    edfi = EdFi(year=year, customer_id=customerid)

    for endpoint in edfi.get_endpoints():
        try:
            stats[endpoint] = True if edfi.get(endpoint, 0, 1) else False
        except:
            stats[endpoint] = False
    echo(json.dumps(stats, indent=4), PASS)

@cli.command()
def version():
    """ prints version """
    print("edfi cli v%s" % __version__)

if __name__ == '__main__':
    cli()

