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

        mod_flow_entry(datapath, {}, ofproto.OFPFC_DELETE)
        priority = 100
        self.logger.info('switch joind: datapath: %061x' % datapath.id) 
        mod_flow_entry(datapath,
                      {'priority' : priority,
                       'match' : {'in_port' : 1},
                       'actions' : [{'type' : 'SET_FIELD',
                                     'field': 'ipv4_dst',
                                     'value': '172.16.0.1'},
                                    {'type' : 'OUTPUT', 'port' : 2},
                                    {'type' : 'SET_FIELD',
                                     'field' : 'ipv4_dst',
                                     'value' : '10.0.0.1'},
                                    {'type' : 'OUTPUT', 'port' : 3},
                                    {'type' : 'SET_FIELD',
                                     'field': 'ipv4_dst',
                                     'value' : '192.168.0.1'},
                                    {'type' : 'OUTPUT', 'port' :2}]},
                      ofproto.OFPFC_ADD)

