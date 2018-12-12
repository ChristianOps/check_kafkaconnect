#!/usr/bin/env python
# -*- coding: utf-8 *-*

# Copyright 2018 Christian Nilsson / Notably AB

# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at

#   http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

__author__ = 'Christian Nilsson <christian@notably.se>'
__version__ = '0.2'
__plugin_name__ = 'check_kafka_connector.py'

try:
    import requests
    import nagiosplugin as np
    import json
    import argparse
    import logging
    import time
    import operator as op
    import sys
except ImportError, mm:

    print 'Missing module: "%s"' % mm

_log = logging.getLogger('nagiosplugin')


class Connector(np.Resource):

    def __init__(self, args):
        self.url = args.url
        self.connector = args.connector

    def probe(self):

        metrics = []
        try:
            ''' parse the response and return metrics '''
            status, r, api_latency = get_active(self)

            if status is True:
                ''' connector information. '''
                try:
                    _log.info('probe connector> name: %s', r['name'])
                    _log.info('probe connector> type: %s', r['type'])

                    metrics += [np.Metric('connector_name', r['name'],
                                          context='ctx'),
                                np.Metric('connector_type', r['type'],
                                          context='ctx')]
                except KeyError:
                    return np.Metric('The connector \'' + self.connector
                                     + '\' was not found', -2, context='ctx')

                ''' connector status. '''
                try:
                    qc = r['connector']

                    _log.info('probe connector> state: %s', qc['state'])

                    metrics += [np.Metric('connector_state', qc['state'],
                                          context='ctx'),
                                np.Metric('connector_wrkid', qc['worker_id'],
                                          context='ctx'),
                                np.Metric('api_latency', api_latency,
                                          's', min=0, context='api_latency')]
                except KeyError:
                    return np.Metric('The connector \'' + self.connector
                                     + '\' was not found', -2, context='ctx')

                ''' connector tasks. '''
                try:

                    qt = r['tasks']
                    _log.debug('probe task> list: %s', qt)

                    ''' count the number of tasks for worker_id. '''
                    wrkids = {}
                    for wrkid in qt:
                        try:
                            if wrkid['worker_id'] in wrkids:
                                wrkids[wrkid['worker_id']] += 1
                            else:
                                wrkids[wrkid['worker_id']] = 1

                        except KeyError:
                            return np.Metric('Failed to get task(s) '
                                             'information', -2,
                                             context='ctx')
                    top_worker = max(wrkids.iteritems(),
                                     key=op.itemgetter(1))[0]
                    top_count = max(wrkids.iteritems(),
                                    key=op.itemgetter(1))[1]

                    ''' count number of running tasks. '''
                    li = [x['state'] for x in qt]
                    running = li.count('RUNNING')
                    _log.info('probe task> tasks running: %s',
                              running)

                    metrics += [
                        np.Metric('running_tasks', int(running),
                                  min=0, max=int(len(qt)),
                                  context='running_tasks'),
                        np.Metric('worker_task_count',
                                  top_count, min=0, max=int(len(qt)),
                                  context='worker_task_count'),
                        np.Metric('worker_name', top_worker, context='ctx')]

                except KeyError:
                    return np.Metric('No tasks found in response', -2,
                                     context='ctx')
            else:
                return np.Metric(format(r), -1, context='ctx')
        except IOError as e:
            return np.Metric(format(e), -1, context='ctx')
        except ValueError as e:
            return np.Metric(format(e), -1, context='ctx')
        except KeyError as e:
            return np.Metric(format(e), -1, context='ctx')
        except AttributeError as e:
            return np.Metric(format(e), -1, context='ctx')

        return metrics


class Context(np.Context):

    def evaluate(self, metric, resource):

        ''' Custom evaluation logic for none-threshold responses. '''

        if metric.value == -1:
            return self.result_cls(np.Critical, metric=metric)

        if metric.value == -2:
            return self.result_cls(np.Critical, metric=metric)

        if metric.name == 'connector_state':
            if metric.value == 'RUNNING':
                _log.debug('context eval> connector: %s', metric.value)
                return self.result_cls(np.Ok, metric=metric)

            if metric.value != 'RUNNING':
                _log.debug('context eval> connector: %s', metric.value)
                return self.result_cls(np.Critical, metric=metric,
                                       hint='connector is in an '
                                       + metric.value + ' state')

        else:
            return self.result_cls(np.Ok, metric=metric)

    def describe(self, metric, state=None):

        ''' Provides a human-readable metric description. '''

        if metric.value == -1:
            return 'Kafka Connect nodes are not responding!'

        if metric.value == -2:
            return metric.name


class Summary(np.Summary):

    ''' summary class for customized output. '''

    def ok(self, results):

        if len(results) > 1:
            result = dict((i.metric.name, i.metric.value) for i in results)
            msg = result.get('connector_name') + ' ('\
                + result.get('connector_wrkid') + ') ' + 'is '\
                + result.get('connector_state').lower()\
                + ' with ' + str(result.get('running_tasks'))\
                + ' task(s).'
            return msg
        else:
            return super(Summary, self).ok(results)

    def problem(self, results):

        if len(results) > 1:
            if results['connector_state'].state == np.state.Critical:
                return super(Summary, self).problem(results)
            if results['worker_task_count'].state == np.state.Critical or\
                    results['worker_task_count'].state == np.state.Warn:
                result = dict((i.metric.name, i.metric.value) for i in results)
                msg = result.get('worker_name') + ' is running '\
                    + str(result.get('worker_task_count')) + ' task(s)'
                return (msg)
            else:
                result = dict((i.metric.name, i.metric.value) for i in results)
                return '{0}'.format(results.first_significant)
        else:
            return super(Summary, self).problem(results)


def connector_list(args):
    url = 'http://' + args.url + '/connectors'
    header = {'Content-type': 'application/json',
              'Accept': 'application/json'}
    try:
        r = requests.get(url, headers=header, timeout=3)
        _log.debug('connector_list> response code: %s', r.status_code)
        return True, json.loads(r.text)
    except requests.HTTPError as e:
        return False, e
    except requests.ConnectionError as e:
        return False, e
    except requests.exceptions.Timeout as e:
        return False, e


def get_status(args):

    ''' called from get_active. '''

    start = time.time()
    url = 'http://' + args.url + '/connectors/'\
          + args.connector + '/status'
    header = {'Content-type': 'application/json',
              'Accept': 'application/json'}

    try:
        sr = requests.get(url, headers=header, timeout=3)
        _log.debug('get_status> response code: %s', sr.status_code)
        return True, json.loads(sr.text), round(time.time() - start, 3)
    except requests.HTTPError as e:
        return False, e, round(time.time() - start, 3)
    except requests.ConnectionError as e:
        return False, e, round(time.time() - start, 3)
    except requests.exceptions.Timeout as e:
        return False, e, round(time.time() - start, 3)


def get_active(args):

    ''' iterate urls for finding a active node. '''

    urls = [str(item) for item in args.url.split(',')]

    _log.debug('get_active> ---> check iteration start <---')
    for (i, val) in enumerate(urls):
        cur_iter = i + 1
        max_iter = len(urls)
        _log.info('checking node: %s / %s - %s' % (cur_iter, max_iter, val))
        args.url = val
        if val:
            status, r, api_latency = get_status(args)
            if status is True:
                _log.info('node is active')
                break
            elif (cur_iter == max_iter):
                _log.info('url list exhausted')
                break
            elif status is False:
                _log.info('node is inactive')
                _log.debug('get_active> node is inactive response: %s' % r)

        else:
            raise np.CheckError('Invalid URL: {0}'.format(val))

    return status, r, api_latency


def create_check(args):

    check = np.Check(
        Connector(args),
        np.ScalarContext('api_latency', args.warning[0], args.critical[0],
                         fmt_metric='api responsetime is {value}{uom}'),
        np.ScalarContext('running_tasks', args.warning[1], args.critical[1],
                         fmt_metric='{value}/{max} running task(s)'),
        np.ScalarContext('worker_task_count', args.warning[2],
                         args.critical[2]),

        Context('ctx'),
        Summary())

    return check


def parse_args():

    argp = argparse.ArgumentParser(description=__doc__)
    conn = argp.add_argument_group('Connection')
    conn.add_argument('-u', '--url', metavar='<url>', required=True,
                      help='URL of API')
    atrib = argp.add_argument_group('Attributes')
    atrib.add_argument('-C', '--connector', required=False,
                       metavar='<connector name>',
                       help='name of connector to check')
    atrib.add_argument('-w', '--warning', metavar='<warn range>',
                       help='warning threshold or range',
                       required='--connector' in sys.argv)
    atrib.add_argument('-c', '--critical', metavar='<crit range>',
                       help='critical threshold or range',
                       required='--connector' in sys.argv)
    atrib.add_argument('-t', '--timeout', metavar='<timeout>',
                       default=10,
                       help='timeout in seconds (default: %(default)s)')
    info = argp.add_argument_group('Info')
    info.add_argument('-l', '--list', action='store_true',
                      help='list available connectors')
    info.add_argument('-v', '--verbose',
                      action='count', default=0,
                      help='increase output verbosity (up to 3 times)')
    info.add_argument('-V', '--version', action='version',
                      version=__version__)

    return argp.parse_args()


@np.guarded
def main():

    args = parse_args()

    if not args.list:
        if args.warning and args.critical:
            warn_args = args.warning.split(',')
            crit_args = args.critical.split(',')
            args.warning = warn_args
            args.critical = crit_args

        ''' Create the check. '''
        check = create_check(args)
        check.main(verbose=args.verbose, timeout=args.timeout)
    else:
        status, response = connector_list(args)
        if status:
            print("\n".join(response))


if __name__ == '__main__':
    main()
