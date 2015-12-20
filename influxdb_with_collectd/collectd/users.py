#!/usr/bin/python
# -*- coding: utf-8 -*-


import collectd
import MySQLdb as mdb

# Global variables
NAME = 'users'
VERBOSE = True


def get_data():
    con = mdb.connect(host="example.com", port=3306, user="monitor", passwd="123abc", db="box")
    cur = con.cursor()
    cur.execute("SELECT offline,online,total from users;)")
    row = cur.fetchone()
    x = row[0]
    y = row[1]
    cur.close()
    con.close()
    status = ['Offline', 'Online', 'Total']
    counts = [x, y, (x+y)]
    return zip(status, counts)

# Helper functions


def log(t, message):
    """ Log messages to collectd logger
    """
    if t == 'err':
        collectd.error('{}: {}'.format(NAME, message))
    elif t == 'warn':
        collectd.warning('{}: {}'.format(NAME, message))
    elif t == 'verb':
        if VERBOSE:
            collectd.info('{}: {}'.format(NAME, message))
    else:
        collectd.info('{}: {}'.format(NAME, message))


def configure_callback(conf):
    """ Config data from collectd
    """
    log('verb', 'configure_callback Running')
    global NAME, VERBOSE
    for node in conf.children:
        if node.key == 'Name':
            NAME = node.values[0]
        elif node.key == 'Verbose':
            if node.values[0] == 'False':
                VERBOSE = False
        else:
            log('warn', 'Unknown config key: {}'.format(node.key))


def read_callback():
    """ Prepare data for collectd
    """
    log('verb', 'read_callback Running')

    stats = get_data()

    if not stats:
        log('verb', 'No statistics received')
        return

    for status, counts in stats:
        log('verb', 'Sending value: {} {}'.format(status, counts))
        value = collectd.Values(plugin=NAME)
        value.type = 'gauge'
        value.type_instance = status
        value.values = [str(counts)]
        value.dispatch()

# Register to collectd
collectd.register_config(configure_callback)
collectd.warning('Initialising {}'.format(NAME))
collectd.register_read(read_callback)
