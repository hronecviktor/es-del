#!/usr/bin/python3
import argparse, requests, datetime, re
from urllib import parse


def get_args():
    parser = argparse.ArgumentParser(description='Selectively deletes data from ElasticSearch cluster')
    parser.add_argument('-i', '--index', help='index to delete from, *REQUIRED*',
                        action='store', required=True)
    parser.add_argument('-d', '--dtype', help='''data types to delete from, will delete from all data types if not
    provided''', action='store')
    parser.add_argument('-s', '--server', help='''IP or hostname and port of ES cluster to delete from, e.g.
    localhost:9200 Default: localhost/127.0.0.1:9200''', action='store', default='localhost:9200')
    mx1 = parser.add_mutually_exclusive_group()
    mx1.add_argument('-f', '--from-stamp', help='''Delete from entered timestamp in ES format, e.g.:
    2014-07-23T00:00:00.000Z''', action='store')
    mx1.add_argument('-F', '--from-ago', help='''Delete from a generated timestamp relative to current time. E.g.:
    '30s','15m','24h','7d' - x {seconds,minutes,hours,days} ago''', action='store')
    mx2 = parser.add_mutually_exclusive_group()
    mx2.add_argument('-t', '--to-stamp', help='''Delete up to entered timestamp in ES format, e.g.: 2014-07-23T00:00:00.
    000Z''', action='store')
    mx2.add_argument('-T', '--to-ago', help='''Delete up to a generated timestamp relative to current time. E.g.:
    '30s','15m','24h','7d' - x {seconds,minutes,hours,days} ago''', action='store')
    parser.add_argument('-n', '--noconfirm', help='Do not require user confirmation. (cron,scripts)', action='store_true')
    parser.add_argument('-q', '--query-only', help='Do not commit the delete, print the query and exit', action='store_true')
    args = parser.parse_args()
    return args


def parse_time(time_val):
    if not time_val:
        return None
    pat = re.compile('^(\d+)(\w)')
    times = pat.findall(time_val)
    times = [int(times[0][0]), times[0][1].lower()]
    if len(times) < 2 or int(times[0]) < 1 or times[1].lower() not in ['s', 'm', 'h', 'd']:
        print("Improper timedelta: {}; use format n{h,d}")
        exit(0)
    gethours = lambda x: x[0] if x[1].lower() == 'h' else (x[0]/60 if x[1].lower() == 'm' else (x[0]/3600 if x[1].lower() == 's' else x[0]*24))
    desired_date = datetime.datetime.now() - datetime.timedelta(hours=gethours(times))
    esformatdate = desired_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    return esformatdate


def validate_tstamp(tstamp):
    if not tstamp:
        return None
    try:
        datetime.datetime.strptime(tstamp, '%Y-%m-%dT%H:%M:%S.000Z')
        return tstamp
    except ValueError:
        print("Invalid timestamp: \'{}\' is not a valid date.".format(tstamp))
        exit(0)


def form_url(args):
    url = 'http://{server}{uri}{endpoint}{query}'
    geturi = lambda index, dtype: '/{}/'.format(index) if index and not dtype else ('/*/{}/'.format(dtype) if dtype and not index else ('/{}/{}/'.format(index, dtype) if index and dtype else '/'))
    getendpoint = lambda datefrom, dateto: '_query?pretty&q=' if datefrom or dateto else ''
    getquery = lambda datefrom, dateto: '+@timestamp:>{}'.format(datefrom.replace(":", '''\:''')) if datefrom and not dateto else ('+@timestamp:<{}'.format(dateto.replace(":", '''\:''')) if dateto and not datefrom else ('+@timestamp:>{} +@timestamp:<{}'.format(datefrom.replace(":", '''\:'''), dateto.replace(":", '''\:''')) if datefrom and dateto else ''))
    url = url.format(server=args.server or 'localhost:9200', uri=geturi(args.index, args.dtype), endpoint=getendpoint(validate_tstamp(args.from_stamp) or parse_time(args.from_ago), validate_tstamp(args.to_stamp) or parse_time(args.to_ago)), query=getquery(validate_tstamp(args.from_stamp) or parse_time(args.from_ago), validate_tstamp(args.to_stamp) or parse_time(args.to_ago)))
    url = "/".join(url.split("/")[:-1])+"/"+parse.quote(url.split("/")[-1], safe='''\/@:=&?<>''')
    return url


def confirm(url):
    print("Generated url is:\n{}".format(url))
    url = url.replace('query','search')
    headers = {'content-type': 'application/json'}
    response = requests.get(url, headers=headers)
    reg_total = re.compile('''"total" : (\d+)''')
    totals = reg_total.findall(str(response.content))[1]
    choice = input("The query will delete {} records. Commit? y/n\n".format(totals)).lower()
    if not choice.startswith('y'):
        exit(0)
    return


if __name__ == '__main__':
    arguments = get_args()
    query_url = form_url(arguments)
    if arguments.query_only:
        print(query_url)
        exit(0)
    if not arguments.noconfirm:
        confirm(query_url)
    headers = {'content-type': 'application/json'}
    response = requests.delete(query_url, headers=headers)