from scapy.all import IP, sniff
from scapy.layers import http

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
    #print '{0[src]},{0[dst]},{1[Method]},{1[Host]},{1[Path]}'.format(ip_layer.fields, http_layer.fields)
    #print http_layer.fields["User-Agent"]
    for key in http_layer.fields:
        print key, http_layer.fields[key]

# Start sniffing the network.
sniff(iface='enp0s3', filter='tcp', prn=process_tcp_packet)
