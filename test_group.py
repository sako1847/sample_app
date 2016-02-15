import time
import socket

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.ofctl_v1_3 import mod_flow_entry

class SAMPLE_APP(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SAMPLE_APP, self).__init__(*args, **kwargs)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        mod_flow_entry(datapath, {}, ofproto.OFPFC_DELETE)
        priority = 100
        self.logger.info('switch joind: datapath: %061x' % datapath.id)
        action = []
        buckets = []
        action.append(parser.OFPActionSetField(eth_src="00:00:00:00:00:00"))
        action.append(parser.OFPActionOutput(1, 0))
        buckets.append(parser.OFPBucket(0,0,0,actions = action))
        action = []
        action.append(parser.OFPActionSetField(eth_src="00:00:00:00:00:01"))
        action.append(parser.OFPActionOutput(2, 0))
        buckets.append(parser.OFPBucket(0,0,0,actions = action))
        mod = parser.OFPGroupMod(datapath, ofproto.OFPGC_ADD, ofproto.OFPGT_ALL, 1, buckets)
        datapath.send_msg(mod)

        match = parser.OFPMatch(in_port = 1, eth_type = 0x800, ip_proto = socket.IPPROTO_UDP, udp_dst = 63)
        action = []
        action.append(parser.OFPActionGroup(group_id = 1))
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, action)]
        mod = parser.OFPFlowMod(datapath = datapath, priority = 100, match = match,
                                instructions = inst)
        datapath.send_msg(mod)

