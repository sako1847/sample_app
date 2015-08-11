import time
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.ofproto.ether import ETH_TYPE_IP
from ryu.lib.ofctl_v1_3 import mod_flow_entry

class Flow_Delete_by_Cookie(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(Flow_Delete_by_Cookie, self).__init__(*args, **kwargs)
    
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto 

        mod_flow_entry(datapath, {}, ofproto.OFPFC_DELETE)
        priority = 100
        self.logger.info('switch joind: datapath: %061x' % datapath.id) 
        mod_flow_entry(datapath,
                      {'priority' : priority,
                       'cookie' : 1234605616436508552,
                       'cookie_mask': 0,
                       'match' : {'dl_type' : ETH_TYPE_IP,
                                  'ipv4_dst' : '192.168.1.1'},
                       'actions' : [{'type' : 'OUTPUT', 'port' : 2}]},
                      ofproto.OFPFC_ADD)

        self.logger.info("----flow mod----")
        time.sleep(10)
        self.logger.info("---flow delete---")
        mod_flow_entry(datapath, {'cookie':1234605616436508552},
                      ofproto.OFPFC_DELETE)
