#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Example Kafka Producer.
# Reads lines from stdin and sends to Kafka.
#

from confluent_kafka import Producer
import sys
from scapy.all import IP, sniff
from scapy.layers import http


if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    topic  = sys.argv[2]

    # Producer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {'bootstrap.servers': broker}

    # Create Producer instance
    p = Producer(**conf)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback (err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d]\n' % \
                             (msg.topic(), msg.partition()))


    def process_tcp_packet(packet):
        '''
        Processes a TCP packet, and if it contains an HTTP request, it prints it.
        '''
        if not packet.haslayer(http.HTTPRequest):
        # This packet doesn't contain an HTTP request so we skip it
            return
        http_layer = packet.getlayer(http.HTTPRequest)
        ip_layer = packet.getlayer(IP)
        # print '\n{0[src]} just requested a {1[Method]} {1[Host]}{1[Path]}'.format(ip_layer.fields, http_layer.fields)
        print '{0[src]},{0[dst]},{1[Method]},{1[Host]},{1[Path]}'.format(ip_layer.fields, http_layer.fields)
        #print http_layer.fields["User-Agent"]
        for key in http_layer.fields:
            print key, http_layer.fields[key]

    # Start sniffing the network.
    #sniff(iface='enp0s3', filter='tcp', prn=process_tcp_packet)

    # Read lines from stdin, produce each line to Kafka
    for line in sniff(iface='enp0s3', filter='tcp', prn=process_tcp_packet):
        try:
            # Produce line (without newline)
            p.produce(topic, line.rstrip(), callback=delivery_callback)
            
        except BufferError as e:
            sys.stderr.write('%% Local producer queue is full ' \
                             '(%d messages awaiting delivery): try again\n' %
                             len(p))

        # Serve delivery callback queue.
        # NOTE: Since produce() is an asynchronous API this poll() call
        #       will most likely not serve the delivery callback for the
        #       last produce()d message.
        p.poll(0)

    # Wait until all messages have been delivered
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()
