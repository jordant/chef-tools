#!/usr/bin/env python
import os
import optparse
import time
import random
import sys
import chef
import pssh
from itertools import islice


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


def get_nodes_from_search():
    nodes = {}
    try:
        api = chef.ChefAPI.from_config_file(options.knife)
        search = chef.Search('node', options.search)
        for h in search:
            nodes[h.object.attributes['ipaddress']] = h.object.attributes['fqdn']
    except Exception as e:
        print e

    return nodes


def log(nodes, output):
    for host in output:
        if options.log:
            f = open(options.log_dest + "/" + nodes[host] + '.log', 'a')
            f.write('STDOUT : \n')
            f.write('\n'.join(output[host]['stdout']))
            f.write('STDERR : \n')
            f.write('\n'.join(output[host]['stderr']))
            f.close()


def chunks(data, SIZE=options.sshpool):
    it = iter(data)
    for i in xrange(0, len(data), SIZE):
        yield {k: data[k] for k in islice(it, SIZE)}


def ips_to_fqdn(nodes, ips):
    hosts = []
    for ip in ips:
        hosts.append(nodes[ip])
    return hosts
        

def print_seperator():
    print '=' * 75

nodes = get_nodes_from_search()

print "Executing command : %s" % command
print "Node(s) found: %s (%s)" % (len(nodes), options.search)

failed = []
chunk_num = 0
total_chunks = (len(nodes) / options.sshpool)
for node_chunk in chunks(nodes, options.sshpool):
    chunk_num += 1
    host_ips = node_chunk.keys()
    for retry in xrange(1, options.retries + 1):
        if not len(host_ips):
            break
        print "Chunk %d of %d .. Attempt %d ... Hosts %s" % (chunk_num, total_chunks, retry, len(host_ips))
        print "Hosts: (%s)" % ', '.join(ips_to_fqdn(node_chunk, host_ips))
        print_seperator()
        output = None
        try:
            client = pssh.ParallelSSHClient(host_ips,
                                            pool_size=options.sshpool)
            if options.sudo:
                output = client.run_command(command, sudo=True)
            else:
                output = client.run_command(command, sudo=False)
        except pssh.ConnectionErrorException as e:
            print "SSH EXCEPTION: %s" % e

        if output:
            log(node_chunk, output)
            for o in output:
                exit_code = client.get_exit_code(output[o])
                if exit_code == 0:
                    host_ips.remove(o)
                else:
                    print "\tERROR: host: %s exit: %s" % (node_chunk[o], exit_code)

    if len(host_ips):
        print "Failures (%s)" % ', '.join(ips_to_fqdn(node_chunk, host_ips))
        for failed_ip in host_ips:
            failed.append(failed_ip)
    else:
        print "Complete"


print_seperator()

print failed
if len(failed):
    print "Failures (%s)" % ', '.join(ips_to_fqdn(nodes, failed))
    sys.exit(1)
else:
    print "Success!"
    sys.exit(0)
