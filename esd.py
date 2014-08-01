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
    parser.add_argument('-c', '--confirm', help='Ask for confirmation', action='store_true')
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
    parser.add_argument('-v', '--verbose', help='Display additional output', action='store_true')
    #PARSE IT
    args = parser.parse_args()
    print(args)
    return args


def parse_time(time_val):
    if not time_val:
        return None
    # 2014-07-26T00\:00\:00.000Z
    pat = re.compile('^(\d+)(\w)')
    times = pat.findall(time_val)
    times = [int(times[0][0]), times[0][1].lower()]
    print(times)
    # print(len(times) < 2, int(times[0])<1, times[1].lower()not in ['h', 'd'])
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
    #curl -XDELETE 'http://localhost:9200/net_log_index/net_log/_query?q=%2b@timestamp%3a>2014-07-23T00\:00\:00.000Z%20%2b@timestamp%3a<2014-07-26T00\:00\:00.000Z&pretty&from=1'
    url = 'http://{server}{uri}{endpoint}{query}'
    geturi = lambda index, dtype: '/{}/'.format(index) if index and not dtype else ('/*/{}/'.format(dtype) if dtype and not index else ('/{}/{}/'.format(index, dtype) if index and dtype else '/'))
    getendpoint = lambda datefrom, dateto: '_query?pretty&q=' if datefrom or dateto else ''
    getquery = lambda datefrom, dateto: '+@timestamp:>{}'.format(datefrom.replace(":", '''\:''')) if datefrom and not dateto else ('+@timestamp:<{}'.format(dateto.replace(":", '''\:''')) if dateto and not datefrom else ('+@timestamp:>{} +@timestamp:<{}'.format(datefrom.replace(":", '''\:'''), dateto.replace(":", '''\:''')) if datefrom and dateto else ''))
    url = url.format(server=args.server or 'localhost:9200', uri=geturi(args.index, args.dtype), endpoint=getendpoint(validate_tstamp(args.from_stamp) or parse_time(args.from_ago), validate_tstamp(args.to_stamp) or parse_time(args.to_ago)), query=getquery(validate_tstamp(args.from_stamp) or parse_time(args.from_ago), validate_tstamp(args.to_stamp) or parse_time(args.to_ago)))
    url = "/".join(url.split("/")[:-1])+"/"+parse.quote(url.split("/")[-1], safe='''\/@:=&?<>''')
    print(url)
    return url

if __name__ == '__main__':
    a = get_args()
    url = form_url(a)
    headers = {'content-type': 'application/json'}
    response = requests.delete(url, headers=headers)
    print(response.content)
    # print(validate_tstamp('''2014-07-26T00:00:00.000Z'''))

# http://localhost:9200/net_log_index/net_log/_query?from=1&q=%2B@timestamp:>2014-07-31T14\:39\:18.000Z%20%2B@timestamp:<2014-08-31T15\:39\:18.000Z&pretty