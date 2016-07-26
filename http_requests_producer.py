from scapy.all import IP, sniff
from scapy.layers import http
from confluent_kafka import Producer

options = dict()

if len(sys.argv) != 3:
    sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
    sys.exit(1)

options["broker"] = sys.argv[1]
options["topic"] = sys.argv[2]

def http_requests_producer(options):

    # Producer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {'bootstrap.servers': options["broker"]}

    # Create Producer instance
    p = Producer(**conf)

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
        ahttp_request_list = [ip_layer.fields["src"],ip_layer.fields["dst"],http_layer.fields["Method"],http_layer.fields["Host"],http_layer.fields["Path"]]

        try:
            # Produce line (without newline)
            p.produce(options["topic"], ahttp_request_list, callback=delivery_callback)
            
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


        return ahttp_request_list

        # End of process_tcp_packets

    return process_tcp_packet

# Start sniffing the network.
sniff(iface='enp0s3', filter='tcp', prn=http_requests_producer(options))
