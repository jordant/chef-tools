#!/usr/bin/env python
# search chef for nodes and run concurrent
# ssh commands across them

import optparse
import time
import sys
import chef
import pssh

parser = optparse.OptionParser()
parser.add_option('--knife', '-k', default='~/.chef/knife.rb', action='store')
parser.add_option('--search', '-s', default='role:*', action='store')
parser.add_option('--command', '-c', default='uptime', action='store')
parser.add_option('--sshpool', '-p', type='int', default=10, action='store')
parser.add_option('--retries', '-r', type='int', default=3, action='store')

options, remainder = parser.parse_args()

api = chef.ChefAPI.from_config_file(options.knife)
nodes = chef.Search('node', options.search)


def log(output):
    for host in output:
        stdout = "\n".join(output[host]['stdout'])
        stderr = "\n".join(output[host]['stderr'])
        node_name = ip_to_fqdn(host)
        f = open(node_name + '.log', 'w')
        f.write(stderr)
        f.write(stdout)
        f.close()


def ip_to_fqdn(host):
    return (n.object.attributes['fqdn'] for n in nodes if n.object.attributes['ipaddress'] == host).next()

hosts = []
for h in nodes:
    try:
        hosts.append(h.object.attributes['ipaddress'])
    except:
        print "%s has no ipaddress" % h.object.name

failed_hosts = []
errors = []
for retry in xrange(options.retries):
    errors = []
    output = []

    run_hosts = hosts
    if failed_hosts:
        run_hosts = failed_hosts

    try:
        client = pssh.ParallelSSHClient(run_hosts, pool_size=options.sshpool)
    except pssh.ConnectionErrorException, e:
        errors.append(e)
        continue

    try:
        output = client.run_command(options.command, sudo=True)
    except pssh.ConnectionErrorException as e:
        errors.append(e)
        continue

    log(output)

    failed_hosts = []
    for host in output:
        exit_code = client.get_exit_code(output[host])
        if exit_code >= 1:
            failed_hosts.append(host)

    # if we have failures, sleep then retry. if no failures, break
    if failed_hosts:
        time.sleep(10)
    else:
        break

if len(errors):
    print errors
    sys.exit(1)
 
if failed_hosts:
    failed_nodes = []
    for failed in failed_hosts:
        failed_nodes.append(ip_to_fqdn(failed))
    print "Failed to run : %s on %s" % (options.command, ", ".join(failed_nodes))
    sys.exit(1)
else:
    print "Success!"
    sys.exit()
