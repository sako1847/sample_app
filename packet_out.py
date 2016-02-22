import time
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet.arp import arp
from ryu.lib.packet.ethernet import ethernet
from ryu.ofproto import ether

class SAMPLE_APP(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SAMPLE_APP, self).__init__(*args, **kwargs)
    
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto 
        parser = datapath.ofproto_parser
        while True:
          
          pkt = packet.Packet()
          dst_ip = "10.0.0.1"
          src_ip = "10.0.0.2"
          dst_mac = "FF:FF:FF:FF:FF:FF"
          src_mac = "12:34:56:78:90:00"

          pkt.add_protocol(ethernet(ethertype = ether.ETH_TYPE_ARP,
                                    dst = dst_mac,
                                    src = src_mac))

          pkt.add_protocol(arp(opcode = 1,
                           src_mac = src_mac,
                           src_ip = src_ip,
                           dst_mac = dst_mac,
                           dst_ip = dst_ip))

          pkt.serialize()
          self.logger.info("%d (%d) send ARP request:%s" % (datapath.id, 3, pkt))
          data = pkt.data
          actions = [parser.OFPActionOutput(port = 3)]
          out = parser.OFPPacketOut(datapath = datapath, buffer_id = ofproto.OFP_NO_BUFFER,
                                    in_port = ofproto.OFPP_CONTROLLER, actions = actions,
                                    data = data)
          datapath.send_msg(out)

 
          self.logger.info("%d (%d) send ARP request:%s" % (datapath.id, 2, pkt))
          actions = [parser.OFPActionOutput(port = 2)]
          out = parser.OFPPacketOut(datapath = datapath, buffer_id = ofproto.OFP_NO_BUFFER,
                                    in_port = ofproto.OFPP_CONTROLLER, actions = actions,            
                                    data = data)
          datapath.send_msg(out)


          self.logger.info("%d (%d) send ARP request:%s" % (datapath.id, 1, pkt))
          actions = [parser.OFPActionOutput(port = 1)]
          out = parser.OFPPacketOut(datapath = datapath, buffer_id = ofproto.OFP_NO_BUFFER,
                                    in_port = ofproto.OFPP_CONTROLLER, actions = actions,
                                    data = data)
          datapath.send_msg(out)

          self.logger.info("*****packet out*****")
          time.sleep(3)
