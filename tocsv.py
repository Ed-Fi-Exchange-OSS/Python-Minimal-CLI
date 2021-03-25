import csv
import json
from urllib.parse import urlparse, parse_qs

import arrow
import click

@click.group()
def cli():
    """ to csv stuffs """
    pass

@cli.command()
@click.argument("log", type=click.File())
def convert(log):
    """ converts profile log file to csv """
    
    entries = []
    summaries = []
    for line in log.readlines():
        data = json.loads(line)

        if data['message'].startswith("GET"):
            # "2018-11-29 14:08:38,749",0.234741,GET https://edfi-mich-test-web.southcentralus.cloudapp.azure.com/v2.4.0/api/api/v2.0/2018/calendarDates?offset=50&limit=50
            pr = urlparse(data['message'].split(" ")[1])
            data['endpoint'] = pr.path.split("/")[-1]
            data['offset'] = int(parse_qs(pr.query)['offset'][0])
            data.pop("message")
            entries.append(data)
        # elif data['message'].startswith("get"): # summaries
        #     # example: get disciplineActions (count: 8168)
        #     parts = data['message'].split()
        #     row = {"endpoint": parts[1], 'count':parts[-1][0:-1], 'duration':data['duration']}
        #     summaries.append(row)

    if not summaries:    
        _summary_dict = {}
        for entry in entries:    
            if not entry['endpoint'] in _summary_dict:
                _summary_dict[entry['endpoint']] = {"duration": 0, "count": 0, 'start':arrow.get(0), 'stop':arrow.get(0)}
            _summary_dict[entry['endpoint']]['duration'] += entry['duration']
            _summary_dict[entry['endpoint']]['count'] = max(_summary_dict[entry['endpoint']]['count'], entry['offset'])
            endpoint_time = arrow.get(entry['datetime'])

        
        for endpoint, val in _summary_dict.items():
            row = {"endpoint": endpoint}
            row.update(val)
            summaries.append(row)

    basename = log.name.split(".")[0]
    with open(basename + "-entries.csv", 'w') as c:
         writer = csv.DictWriter(c, [str(x) for x in entries[0].keys()])
         writer.writeheader()
         writer.writerows(entries)
    if summaries:
        with open(basename + "profile-summaries.csv", 'w') as c:
            writer = csv.DictWriter(c, [str(x) for x in summaries[0].keys()])
            writer.writeheader()
            writer.writerows(summaries)


if __name__ == '__main__':
    cli()