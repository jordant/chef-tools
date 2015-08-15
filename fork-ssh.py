#!/usr/bin/env python
import os
import optparse
import time
import random
import sys
import chef
import pssh

parser = optparse.OptionParser()
parser.add_option('--knife', '-k', default='~/.chef/knife.rb', action='store')
parser.add_option('--log', '-l', action="store_true")
parser.add_option('--log-dest', default='./', action='store', dest="log_dest")
parser.add_option('--search', '-s', default='role:*', action='store')
parser.add_option('--sudo', action="store_true", dest="sudo")
parser.add_option('--command', '-c', default='uptime', action='store')
parser.add_option('--splay', type='int', action='store')
parser.add_option('--sshpool', '-p', type='int', default=10, action='store')
parser.add_option('--retries', '-r', type='int', default=3, action='store')

options, remainder = parser.parse_args()

command = options.command
if options.splay:
    command = "sleep $((( RANDOM % " + str(options.splay) + ") + 1 )) ; " + options.command

if not os.path.isfile(options.knife):
    print "%s does not exist" % options.knife
    sys.exit(1)


nodes = []
try:
    api = chef.ChefAPI.from_config_file(options.knife)
    nodes = chef.Search('node', options.search)
except Exception as e:
    print e
    sys.exit(1)

if not len(nodes):
    print "Search: '%s' did not return any results" % options.search
    sys.exit(1)


def log(output):
    for host in output:
        stdout = "\n".join(output[host]['stdout'])
        stderr = "\n".join(output[host]['stderr'])
        node_name = ip_to_fqdn(host)
        if options.log:
            f = open(options.log_dest + "/" + node_name + '.log', 'w')
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

print "Executing command : %s" % command
print "Node(s) found: %s (%s)" % (len(nodes), options.search)

failed_hosts = []
errors = []
for retry in xrange(1, options.retries + 1):
    errors = []
    run_hosts = hosts
    if failed_hosts:
        print "failed hosts %s" % ", ".join(failed_hosts)
        run_hosts = failed_hosts

    if errors:
        print "errors %s" % errors

    print "Attempt %s" % retry
    try:
        client = pssh.ParallelSSHClient(run_hosts, pool_size=options.sshpool)
    except pssh.ConnectionErrorException, e:
        errors.append(e)
        continue

    output = []
    try:
        if options.sudo:
            output = client.run_command(command, sudo=True)
        else:
            output = client.run_command(command, sudo=False)
    except pssh.ConnectionErrorException as e:
        errors.append(e)
        continue

    log(output)

    failed_hosts = []
    errors = []
    for host in output:
        exit_code = client.get_exit_code(output[host])
        if exit_code >= 1:
            failed_hosts.append(host)

    # if we have failures, sleep then retry. if no failures, break
    if failed_hosts:
        time.sleep(5)
    else:
        break

if errors:
    print "Errors: %s" % (errors)
    sys.exit(1)

if failed_hosts:
    failed_nodes = []
    for failed in failed_hosts:
        failed_nodes.append(ip_to_fqdn(failed))

    print "Failed to run : %s on %s" % (command, ", ".join(failed_nodes))
    sys.exit(1)
else:
    print "Success!"
    sys.exit()
