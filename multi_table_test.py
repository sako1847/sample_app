import time
import random
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.ofproto.ether import ETH_TYPE_IP
from ryu.lib.ofctl_v1_3 import mod_flow_entry
from ryu.lib.mac import haddr_to_bin
from ryu.lib.packet import packet

class TTP(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(TTP, self).__init__(*args, **kwargs)
    
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto 
       
        mod_flow_entry(datapath, {}, ofproto.OFPFC_DELETE)
        
#        metadata = 123456789

#        mod_flow_entry(datapath,
#                      {'priority' : 100,
#                       'table_id' : 0,
#                       'cookie':0,
#                       'cookie_mask' :0,
#                       'match' : {'in_port' : 1},
#                       'actions' : [{'type':'WRITE_METADATA', 'metadata':'123456789', 'metadata_mask':'5'},
#                                    {'type':'GOTO_TABLE', 'table_id':1}]}, datapath.ofproto.OFPFC_ADD)

#        mod_flow_entry(datapath,
#                      {'priority' : 100,
#                       'table_id' : 1,
#                       'cookie' : 0,
#                       'cookie_mask' : 0,
#                       'match' : {'metadata':'123456789/15'},
#                       'actions' : [{'type' : 'OUTPUT', 'port' :2}]}, datapath.ofproto.OFPFC_ADD)

##########


        parser = datapath.ofproto_parser

        priority = 100
        match = parser.OFPMatch()
        match.set_in_port(1)
        instructions = [parser.OFPInstructionWriteMetadata(123456789,15), 
                        parser.OFPInstructionGotoTable(1)]
        flow_mod = parser.OFPFlowMod(datapath = datapath, 
                                     table_id=0, 
                                     priority = priority, 
                                     match=match, 
                                     instructions=instructions)
        datapath.send_msg(flow_mod)
###
        output = parser.OFPActionOutput(2,0)
        match = parser.OFPMatch()
#        match.set_in_port()
        instructions = [parser.OFPInstructionGotoTable(2)]
        flow_mod = parser.OFPFlowMod(datapath = datapath,
                                     table_id=1,
                                     priority = priority,
                                     match = match,
                                     instructions = instructions)
        datapath.send_msg(flow_mod)
        
        match = parser.OFPMatch()
#        match.set_in_port(1)
        actions = [parser.OFPActionOutput(2, ofproto.OFPCML_NO_BUFFER)]
        instructions = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        flow_mod = parser.OFPFlowMod(datapath = datapath, 
                                     table_id=2, 
                                     priority=priority, 
                                     match=match, 
                                     instructions=instructions)
        datapath.send_msg(flow_mod)


#        print pkt    
         
