#!/usr/bin/env python
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
    splay = int(random.randrange(0, options.splay))
    command = "sleep $((( RANDOM % " + str(splay) + ") + 1 )) ; " + options.command

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
print "Hosts found with search : %s  , %s" % (options.search, len(nodes))

failed_hosts = []
for retry in xrange(options.retries):
    run_hosts = hosts
    if failed_hosts:
        run_hosts = failed_hosts

    try:
        client = pssh.ParallelSSHClient(run_hosts, pool_size=options.sshpool)
    except pssh.ConnectionErrorException, e:
        print e
        continue

    output = []
    try:
        if options.sudo:
            output = client.run_command(command, sudo=True)
        else:
            output = client.run_command(command, sudo=False)
    except pssh.ConnectionErrorException as e:
        print e
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
 
if failed_hosts:
    failed_nodes = []
    for failed in failed_hosts:
        failed_nodes.append(ip_to_fqdn(failed))

    print "Failed to run : %s on %s" % (command, ", ".join(failed_nodes))
    sys.exit(1)
else:
    print "Success!"
    sys.exit()
