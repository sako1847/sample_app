from operator import attrgetter

import time
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER,MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.ofproto.ether import ETH_TYPE_IP
from ryu.lib.ofctl_v1_3 import mod_flow_entry
from ryu.lib import hub

class show_port_stats(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(show_port_stats, self).__init__(*args, **kwargs)
        self.datapaths={}
        self.monitor = hub.spawn(self.request_stats)
    
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto 
        self.datapaths[datapath.id] = datapath
        mod_flow_entry(datapath, {}, ofproto.OFPFC_DELETE)
        priority = 100
        self.logger.info('switch joind: datapath: %061x' % datapath.id) 
        mod_flow_entry(datapath,
                      {'priority' : priority,
                       'match' : {'in_port' : 1},
                       'actions' : [{'type' : 'OUTPUT', 'port' : 2}]},
                      ofproto.OFPFC_ADD)

    def request_stats(self):
        while True:
          print ("----------request stats----------")
          for dp in self.datapaths.values():
            ofproto = dp.ofproto
            parser = dp.ofproto_parser
   
            req = parser.OFPFlowStatsRequest(dp)
            dp.send_msg(req)

            req = parser.OFPPortStatsRequest(dp, 0, ofproto.OFPP_ANY)
            dp.send_msg(req)
          hub.sleep(5)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def flow_stats_reply_handler(self,ev):
        body = ev.msg.body
        print("send flow stats reply")

        self.logger.info('---------------------------------------------')
        for stat in sorted([flow for flow in body if flow.priority == 100],
                           key=lambda flow:(flow.match['in_port'])):
          self.logger.info("datapath_id=%d in_port=%d packet_count=%d byte_count=%d",ev.msg.datapath.id,stat.match['in_port'],stat.packet_count,stat.byte_count)

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def port_stats_reply_handler(self, ev):
        body = ev.msg.body
        print("send port stats reply")
        print body
        self.logger.info('---------------------------------------------')

        for stat in sorted (body, key =attrgetter('port_no')):
            self.logger.info(stat)
            self.logger.info("datapath_id=%d, in_port=%d, rx_packets=%d, rx_bytes=%d, rx_errors=%d, tx_packets=%d, tx_bytes=%d, tx_errors=%d", ev.msg.datapath.id,stat.port_no,stat.rx_packets,stat.rx_bytes,stat.rx_errors,stat.tx_packets,stat.tx_bytes,stat.tx_errors)
