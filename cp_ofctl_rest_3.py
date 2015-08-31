# Copyright (C) 2012 Nippon Telegraph and Telephone Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

import json
import ast
import time
from webob import Response

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller import dpset
from ryu.controller.handler import MAIN_DISPATCHER, CONFIG_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_0
from ryu.ofproto import ofproto_v1_2
from ryu.ofproto import ofproto_v1_3
from ryu.lib import ofctl_v1_0
from ryu.lib import ofctl_v1_2
from ryu.lib import ofctl_v1_3
from ryu.lib.ofctl_v1_3 import mod_flow_entry
from ryu.app.wsgi import ControllerBase, WSGIApplication

from ryu.ofproto import ether
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.lib.packet import arp

LOG = logging.getLogger('ryu.app.ofctl_rest')

# supported ofctl versions in this restful app
supported_ofctl = {
    ofproto_v1_0.OFP_VERSION: ofctl_v1_0,
    ofproto_v1_2.OFP_VERSION: ofctl_v1_2,
    ofproto_v1_3.OFP_VERSION: ofctl_v1_3,
}

# REST API
#

# Retrieve the switch stats
#
# get the list of all switches
# GET /stats/switches
#
# get the desc stats of the switch
# GET /stats/desc/<dpid>
#
# get flows stats of the switch
# GET /stats/flow/<dpid>
#
# get flows stats of the switch filtered by the fields
# POST /stats/flow/<dpid>
#
# get aggregate flows stats of the switch
# GET /stats/aggregateflow/<dpid>
#
# get aggregate flows stats of the switch filtered by the fields
# POST /stats/aggregateflow/<dpid>
#
# get ports stats of the switch
# GET /stats/port/<dpid>
#
# get queues stats of the switch
# GET /stats/queue/<dpid>
#
# get meter features stats of the switch
# GET /stats/meterfeatures/<dpid>
#
# get meter config stats of the switch
# GET /stats/meterconfig/<dpid>
#
# get meters stats of the switch
# GET /stats/meter/<dpid>
#
# get group features stats of the switch
# GET /stats/groupfeatures/<dpid>
#
# get groups desc stats of the switch
# GET /stats/groupdesc/<dpid>
#
# get groups stats of the switch
# GET /stats/group/<dpid>
#
# get ports description of the switch
# GET /stats/portdesc/<dpid>

# Update the switch stats
#
# add a flow entry
# POST /stats/flowentry/add
#
# modify all matching flow entries
# POST /stats/flowentry/modify
#
# modify flow entry strictly matching wildcards and priority
# POST /stats/flowentry/modify_strict
#
# delete all matching flow entries
# POST /stats/flowentry/delete
#
# delete flow entry strictly matching wildcards and priority
# POST /stats/flowentry/delete_strict
#
# delete all flow entries of the switch
# DELETE /stats/flowentry/clear/<dpid>
#
# add a meter entry
# POST /stats/meterentry/add
#
# modify a meter entry
# POST /stats/meterentry/modify
#
# delete a meter entry
# POST /stats/meterentry/delete
#
# add a group entry
# POST /stats/groupentry/add
#
# modify a group entry
# POST /stats/groupentry/modify
#
# delete a group entry
# POST /stats/groupentry/delete
#
# modify behavior of the physical port
# POST /stats/portdesc/modify
#
#
# send a experimeter message
# POST /stats/experimenter/<dpid>


class StatsController(ControllerBase):
    def __init__(self, req, link, data, **config):
        super(StatsController, self).__init__(req, link, data, **config)
        self.dpset = data['dpset']
        self.waiters = data['waiters']

    def get_dpids(self, req, **_kwargs):
        dps = list(self.dpset.dps.keys())
        body = json.dumps(dps)
        return Response(content_type='application/json', body=body)

    def get_desc_stats(self, req, dpid, **_kwargs):

        if type(dpid) == str and not dpid.isdigit():
            LOG.debug('invalid dpid %s', dpid)
            return Response(status=400)

        dp = self.dpset.get(int(dpid))

        if dp is None:
            return Response(status=404)
        _ofp_version = dp.ofproto.OFP_VERSION

        _ofctl = supported_ofctl.get(_ofp_version, None)
        if _ofctl is not None:
            desc = _ofctl.get_desc_stats(dp, self.waiters)

        else:
            LOG.debug('Unsupported OF protocol')
            return Response(status=501)

        body = json.dumps(desc)
        return Response(content_type='application/json', body=body)

    def get_flow_stats(self, req, dpid, **_kwargs):

        if req.body == '':
            flow = {}

        else:

            try:
                flow = ast.literal_eval(req.body)

            except SyntaxError:
                LOG.debug('invalid syntax %s', req.body)
                return Response(status=400)

        if type(dpid) == str and not dpid.isdigit():
            LOG.debug('invalid dpid %s', dpid)
            return Response(status=400)

        dp = self.dpset.get(int(dpid))

        if dp is None:
            return Response(status=404)

        _ofp_version = dp.ofproto.OFP_VERSION

        _ofctl = supported_ofctl.get(_ofp_version, None)
        if _ofctl is not None:
            flows = _ofctl.get_flow_stats(dp, self.waiters, flow)

        else:
            LOG.debug('Unsupported OF protocol')
            return Response(status=501)

        body = json.dumps(flows)
        return Response(content_type='application/json', body=body)

    def get_aggregate_flow_stats(self, req, dpid, **_kwargs):

        if req.body == '':
            flow = {}

        else:
            try:
                flow = ast.literal_eval(req.body)

            except SyntaxError:
                LOG.debug('invalid syntax %s', req.body)
                return Response(status=400)

        if type(dpid) == str and not dpid.isdigit():
            LOG.debug('invalid dpid %s', dpid)
            return Response(status=400)

        dp = self.dpset.get(int(dpid))

        if dp is None:
            return Response(status=404)

        _ofp_version = dp.ofproto.OFP_VERSION

        _ofctl = supported_ofctl.get(_ofp_version, None)
        if _ofctl is not None:
            flows = _ofctl.get_aggregate_flow_stats(dp, self.waiters, flow)

        else:
            LOG.debug('Unsupported OF protocol')
            return Response(status=501)

        body = json.dumps(flows)
        return Response(content_type='application/json', body=body)

    def get_port_stats(self, req, dpid, **_kwargs):

        if type(dpid) == str and not dpid.isdigit():
            LOG.debug('invalid dpid %s', dpid)
            return Response(status=400)

        dp = self.dpset.get(int(dpid))

        if dp is None:
            return Response(status=404)

        _ofp_version = dp.ofproto.OFP_VERSION

        _ofctl = supported_ofctl.get(_ofp_version, None)
        if _ofctl is not None:
            ports = _ofctl.get_port_stats(dp, self.waiters)

        else:
            LOG.debug('Unsupported OF protocol')
            return Response(status=501)

        body = json.dumps(ports)
        return Response(content_type='application/json', body=body)

    def get_queue_stats(self, req, dpid, **_kwargs):

        if type(dpid) == str and not dpid.isdigit():
            LOG.debug('invalid dpid %s', dpid)
            return Response(status=400)

        dp = self.dpset.get(int(dpid))

        if dp is None:
            return Response(status=404)

        _ofp_version = dp.ofproto.OFP_VERSION

        _ofctl = supported_ofctl.get(_ofp_version, None)
        if _ofctl is not None:
            queues = _ofctl.get_queue_stats(dp, self.waiters)

        else:
            LOG.debug('Unsupported OF protocol')
            return Response(status=501)

        body = json.dumps(queues)
        return Response(content_type='application/json', body=body)

    def get_meter_features(self, req, dpid, **_kwargs):

        if type(dpid) == str and not dpid.isdigit():
            LOG.debug('invalid dpid %s', dpid)
            return Response(status=400)

        dp = self.dpset.get(int(dpid))

        if dp is None:
            return Response(status=404)

        _ofp_version = dp.ofproto.OFP_VERSION
        _ofctl = supported_ofctl.get(_ofp_version, None)

        if _ofctl is not None and hasattr(_ofctl, 'get_meter_features'):
            meters = _ofctl.get_meter_features(dp, self.waiters)

        else:
            LOG.debug('Unsupported OF protocol or \
                request not supported in this OF protocol version')
            return Response(status=501)

        body = json.dumps(meters)
        return Response(content_type='application/json', body=body)

    def get_meter_config(self, req, dpid, **_kwargs):

        if type(dpid) == str and not dpid.isdigit():
            LOG.debug('invalid dpid %s', dpid)
            return Response(status=400)

        dp = self.dpset.get(int(dpid))

        if dp is None:
            return Response(status=404)

        _ofp_version = dp.ofproto.OFP_VERSION
        _ofctl = supported_ofctl.get(_ofp_version, None)

        if _ofctl is not None and hasattr(_ofctl, 'get_meter_config'):
            meters = _ofctl.get_meter_config(dp, self.waiters)

        else:
            LOG.debug('Unsupported OF protocol or \
                request not supported in this OF protocol version')
            return Response(status=501)

        body = json.dumps(meters)
        return Response(content_type='application/json', body=body)

    def get_meter_stats(self, req, dpid, **_kwargs):

        if type(dpid) == str and not dpid.isdigit():
            LOG.debug('invalid dpid %s', dpid)
            return Response(status=400)

        dp = self.dpset.get(int(dpid))

        if dp is None:
            return Response(status=404)

        _ofp_version = dp.ofproto.OFP_VERSION
        _ofctl = supported_ofctl.get(_ofp_version, None)

        if _ofctl is not None and hasattr(_ofctl, 'get_meter_stats'):
            meters = _ofctl.get_meter_stats(dp, self.waiters)

        else:
            LOG.debug('Unsupported OF protocol or \
                request not supported in this OF protocol version')
            return Response(status=501)

        body = json.dumps(meters)
        return Response(content_type='application/json', body=body)

    def get_group_features(self, req, dpid, **_kwargs):

        if type(dpid) == str and not dpid.isdigit():
            LOG.debug('invalid dpid %s', dpid)
            return Response(status=400)

        dp = self.dpset.get(int(dpid))

        if dp is None:
            return Response(status=404)

        _ofp_version = dp.ofproto.OFP_VERSION
        _ofctl = supported_ofctl.get(_ofp_version, None)

        if _ofctl is not None and hasattr(_ofctl, 'get_group_features'):
            groups = _ofctl.get_group_features(dp, self.waiters)

        else:
            LOG.debug('Unsupported OF protocol or \
                request not supported in this OF protocol version')
            return Response(status=501)

        body = json.dumps(groups)
        return Response(content_type='application/json', body=body)

    def get_group_desc(self, req, dpid, **_kwargs):

        if type(dpid) == str and not dpid.isdigit():
            LOG.debug('invalid dpid %s', dpid)
            return Response(status=400)

        dp = self.dpset.get(int(dpid))

        if dp is None:
            return Response(status=404)

        _ofp_version = dp.ofproto.OFP_VERSION
        _ofctl = supported_ofctl.get(_ofp_version, None)

        if _ofctl is not None and hasattr(_ofctl, 'get_group_desc'):
            groups = _ofctl.get_group_desc(dp, self.waiters)

        else:
            LOG.debug('Unsupported OF protocol or \
                request not supported in this OF protocol version')
            return Response(status=501)

        body = json.dumps(groups)
        return Response(content_type='application/json', body=body)

    def get_group_stats(self, req, dpid, **_kwargs):

        if type(dpid) == str and not dpid.isdigit():
            LOG.debug('invalid dpid %s', dpid)
            return Response(status=400)

        dp = self.dpset.get(int(dpid))

        if dp is None:
            return Response(status=404)

        _ofp_version = dp.ofproto.OFP_VERSION
        _ofctl = supported_ofctl.get(_ofp_version, None)

        if _ofctl is not None and hasattr(_ofctl, 'get_group_stats'):
            groups = _ofctl.get_group_stats(dp, self.waiters)

        else:
            LOG.debug('Unsupported OF protocol or \
                request not supported in this OF protocol version')
            return Response(status=501)

        body = json.dumps(groups)
        return Response(content_type='application/json', body=body)

    def get_port_desc(self, req, dpid, **_kwargs):

        if type(dpid) == str and not dpid.isdigit():
            LOG.debug('invalid dpid %s', dpid)
            return Response(status=400)

        dp = self.dpset.get(int(dpid))

        if dp is None:
            return Response(status=404)

        _ofp_version = dp.ofproto.OFP_VERSION

        _ofctl = supported_ofctl.get(_ofp_version, None)
        if _ofctl is not None:
            groups = _ofctl.get_port_desc(dp, self.waiters)

        else:
            LOG.debug('Unsupported OF protocol')
            return Response(status=501)

        body = json.dumps(groups)
        return Response(content_type='application/json', body=body)

    def mod_flow_entry(self, req, cmd, **_kwargs):

        try:
            flow = ast.literal_eval(req.body)

        except SyntaxError:
            LOG.debug('invalid syntax %s', req.body)
            return Response(status=400)

        dpid = flow.get('dpid')

        if type(dpid) == str and not dpid.isdigit():
            LOG.debug('invalid dpid %s', dpid)
            return Response(status=400)

        dp = self.dpset.get(int(dpid))

        if dp is None:
            return Response(status=404)

        if cmd == 'add':
            cmd = dp.ofproto.OFPFC_ADD
        elif cmd == 'modify':
            cmd = dp.ofproto.OFPFC_MODIFY
        elif cmd == 'modify_strict':
            cmd = dp.ofproto.OFPFC_MODIFY_STRICT
        elif cmd == 'delete':
            cmd = dp.ofproto.OFPFC_DELETE
        elif cmd == 'delete_strict':
            cmd = dp.ofproto.OFPFC_DELETE_STRICT
        else:
            return Response(status=404)

        _ofp_version = dp.ofproto.OFP_VERSION
        _ofctl = supported_ofctl.get(_ofp_version, None)
        if _ofctl is not None:
            _ofctl.mod_flow_entry(dp, flow, cmd)
        else:
            LOG.debug('Unsupported OF protocol')
            return Response(status=501)

        return Response(status=200)

    def delete_flow_entry(self, req, dpid, **_kwargs):

        if type(dpid) == str and not dpid.isdigit():
            LOG.debug('invalid dpid %s', dpid)
            return Response(status=400)

        dp = self.dpset.get(int(dpid))

        if dp is None:
            return Response(status=404)

        _ofp_version = dp.ofproto.OFP_VERSION

        if ofproto_v1_0.OFP_VERSION == _ofp_version:
            flow = {}
        else:
            flow = {'table_id': dp.ofproto.OFPTT_ALL}

        _ofctl = supported_ofctl.get(_ofp_version, None)
        if _ofctl is not None:
            _ofctl.mod_flow_entry(dp, flow, dp.ofproto.OFPFC_DELETE)

        else:
            LOG.debug('Unsupported OF protocol')
            return Response(status=501)

        return Response(status=200)

    def mod_meter_entry(self, req, cmd, **_kwargs):

        try:
            flow = ast.literal_eval(req.body)

        except SyntaxError:
            LOG.debug('invalid syntax %s', req.body)
            return Response(status=400)

        dpid = flow.get('dpid')

        if type(dpid) == str and not dpid.isdigit():
            LOG.debug('invalid dpid %s', dpid)
            return Response(status=400)

        dp = self.dpset.get(int(dpid))

        if dp is None:
            return Response(status=404)

        if cmd == 'add':
            cmd = dp.ofproto.OFPMC_ADD
        elif cmd == 'modify':
            cmd = dp.ofproto.OFPMC_MODIFY
        elif cmd == 'delete':
            cmd = dp.ofproto.OFPMC_DELETE
        else:
            return Response(status=404)

        _ofp_version = dp.ofproto.OFP_VERSION
        _ofctl = supported_ofctl.get(_ofp_version, None)

        if _ofctl is not None and hasattr(_ofctl, 'mod_meter_entry'):
            _ofctl.mod_meter_entry(dp, flow, cmd)

        else:
            LOG.debug('Unsupported OF protocol or \
                request not supported in this OF protocol version')
            return Response(status=501)

        return Response(status=200)

    def mod_group_entry(self, req, cmd, **_kwargs):

        try:
            group = ast.literal_eval(req.body)

        except SyntaxError:
            LOG.debug('invalid syntax %s', req.body)
            return Response(status=400)

        dpid = group.get('dpid')

        if type(dpid) == str and not dpid.isdigit():
            LOG.debug('invalid dpid %s', dpid)
            return Response(status=400)

        dp = self.dpset.get(int(dpid))

        if dp is None:
            return Response(status=404)

        if cmd == 'add':
            cmd = dp.ofproto.OFPGC_ADD
        elif cmd == 'modify':
            cmd = dp.ofproto.OFPGC_MODIFY
        elif cmd == 'delete':
            cmd = dp.ofproto.OFPGC_DELETE
        else:
            return Response(status=404)

        _ofp_version = dp.ofproto.OFP_VERSION
        _ofctl = supported_ofctl.get(_ofp_version, None)

        if _ofctl is not None and hasattr(_ofctl, 'mod_group_entry'):
            _ofctl.mod_group_entry(dp, group, cmd)

        else:
            LOG.debug('Unsupported OF protocol or \
                request not supported in this OF protocol version')
            return Response(status=501)

        return Response(status=200)

    def mod_port_behavior(self, req, cmd, **_kwargs):

        try:
            port_config = ast.literal_eval(req.body)

        except SyntaxError:
            LOG.debug('invalid syntax %s', req.body)
            return Response(status=400)

        dpid = port_config.get('dpid')

        if type(dpid) == str and not dpid.isdigit():
            LOG.debug('invalid dpid %s', dpid)
            return Response(status=400)

        port_no = port_config.get('port_no', 0)
        if type(port_no) == str and not port_no.isdigit():
            LOG.debug('invalid port_no %s', port_no)
            return Response(status=400)

        port_info = self.dpset.port_state[int(dpid)].get(port_no)

        if port_info:
            port_config.setdefault('hw_addr', port_info.hw_addr)
            port_config.setdefault('advertise', port_info.advertised)
        else:
            return Response(status=404)

        dp = self.dpset.get(int(dpid))

        if dp is None:
            return Response(status=404)

        if cmd != 'modify':
            return Response(status=404)

        _ofp_version = dp.ofproto.OFP_VERSION

        _ofctl = supported_ofctl.get(_ofp_version, None)
        if _ofctl is not None:
            _ofctl.mod_port_behavior(dp, port_config)

        else:
            LOG.debug('Unsupported OF protocol')
            return Response(status=501)

        return Response(status=200)

    def send_experimenter(self, req, dpid, **_kwargs):

        if type(dpid) == str and not dpid.isdigit():
            LOG.debug('invalid dpid %s', dpid)
            return Response(status=400)

        dp = self.dpset.get(int(dpid))

        if dp is None:
            return Response(status=404)

        try:
            exp = ast.literal_eval(req.body)

        except SyntaxError:
            LOG.debug('invalid syntax %s', req.body)
            return Response(status=400)

        _ofp_version = dp.ofproto.OFP_VERSION
        _ofctl = supported_ofctl.get(_ofp_version, None)

        if _ofctl is not None and hasattr(_ofctl, 'send_experimenter'):
            _ofctl.send_experimenter(dp, exp)

        else:
            LOG.debug('Unsupported OF protocol')
            return Response(status=501)

        return Response(status=200)


class RestStatsApi(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_0.OFP_VERSION,
                    ofproto_v1_2.OFP_VERSION,
                    ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {
        'dpset': dpset.DPSet,
        'wsgi': WSGIApplication
    }

    def __init__(self, *args, **kwargs):
        super(RestStatsApi, self).__init__(*args, **kwargs)
        self.dpset = kwargs['dpset']
        wsgi = kwargs['wsgi']
        self.waiters = {}
        self.data = {}
        self.data['dpset'] = self.dpset
        self.data['waiters'] = self.waiters
        
        self.arpTable = {}    # IP addr : MAC addr
        self.macTable = {} # dpid : {MAC addr : port}
   
        mapper = wsgi.mapper

        wsgi.registory['StatsController'] = self.data
        path = '/stats'
        uri = path + '/switches'
        mapper.connect('stats', uri,
                       controller=StatsController, action='get_dpids',
                       conditions=dict(method=['GET']))

        uri = path + '/desc/{dpid}'
        mapper.connect('stats', uri,
                       controller=StatsController, action='get_desc_stats',
                       conditions=dict(method=['GET']))

        uri = path + '/flow/{dpid}'
        mapper.connect('stats', uri,
                       controller=StatsController, action='get_flow_stats',
                       conditions=dict(method=['GET', 'POST']))

        uri = path + '/aggregateflow/{dpid}'
        mapper.connect('stats', uri,
                       controller=StatsController,
                       action='get_aggregate_flow_stats',
                       conditions=dict(method=['GET', 'POST']))

        uri = path + '/port/{dpid}'
        mapper.connect('stats', uri,
                       controller=StatsController, action='get_port_stats',
                       conditions=dict(method=['GET']))

        uri = path + '/queue/{dpid}'
        mapper.connect('stats', uri,
                       controller=StatsController, action='get_queue_stats',
                       conditions=dict(method=['GET']))

        uri = path + '/meterfeatures/{dpid}'
        mapper.connect('stats', uri,
                       controller=StatsController, action='get_meter_features',
                       conditions=dict(method=['GET']))

        uri = path + '/meterconfig/{dpid}'
        mapper.connect('stats', uri,
                       controller=StatsController, action='get_meter_config',
                       conditions=dict(method=['GET']))

        uri = path + '/meter/{dpid}'
        mapper.connect('stats', uri,
                       controller=StatsController, action='get_meter_stats',
                       conditions=dict(method=['GET']))

        uri = path + '/groupfeatures/{dpid}'
        mapper.connect('stats', uri,
                       controller=StatsController, action='get_group_features',
                       conditions=dict(method=['GET']))

        uri = path + '/groupdesc/{dpid}'
        mapper.connect('stats', uri,
                       controller=StatsController, action='get_group_desc',
                       conditions=dict(method=['GET']))

        uri = path + '/group/{dpid}'
        mapper.connect('stats', uri,
                       controller=StatsController, action='get_group_stats',
                       conditions=dict(method=['GET']))

        uri = path + '/portdesc/{dpid}'
        mapper.connect('stats', uri,
                       controller=StatsController, action='get_port_desc',
                       conditions=dict(method=['GET']))

        uri = path + '/flowentry/{cmd}'
        mapper.connect('stats', uri,
                       controller=StatsController, action='mod_flow_entry',
                       conditions=dict(method=['POST']))

        uri = path + '/flowentry/clear/{dpid}'
        mapper.connect('stats', uri,
                       controller=StatsController, action='delete_flow_entry',
                       conditions=dict(method=['DELETE']))

        uri = path + '/meterentry/{cmd}'
        mapper.connect('stats', uri,
                       controller=StatsController, action='mod_meter_entry',
                       conditions=dict(method=['POST']))

        uri = path + '/groupentry/{cmd}'
        mapper.connect('stats', uri,
                       controller=StatsController, action='mod_group_entry',
                       conditions=dict(method=['POST']))

        uri = path + '/portdesc/{cmd}'
        mapper.connect('stats', uri,
                       controller=StatsController, action='mod_port_behavior',
                       conditions=dict(method=['POST']))

        uri = path + '/experimenter/{dpid}'
        mapper.connect('stats', uri,
                       controller=StatsController, action='send_experimenter',
                       conditions=dict(method=['POST']))

    @set_ev_cls([ofp_event.EventOFPStatsReply,
                 ofp_event.EventOFPDescStatsReply,
                 ofp_event.EventOFPFlowStatsReply,
                 ofp_event.EventOFPAggregateStatsReply,
                 ofp_event.EventOFPPortStatsReply,
                 ofp_event.EventOFPQueueStatsReply,
                 ofp_event.EventOFPMeterStatsReply,
                 ofp_event.EventOFPMeterFeaturesStatsReply,
                 ofp_event.EventOFPMeterConfigStatsReply,
                 ofp_event.EventOFPGroupStatsReply,
                 ofp_event.EventOFPGroupFeaturesStatsReply,
                 ofp_event.EventOFPGroupDescStatsReply,
                 ofp_event.EventOFPPortDescStatsReply
                 ], MAIN_DISPATCHER)
    def stats_reply_handler(self, ev):
        msg = ev.msg
        dp = msg.datapath

        if dp.id not in self.waiters:
            return
        if msg.xid not in self.waiters[dp.id]:
            return
        lock, msgs = self.waiters[dp.id][msg.xid]
        msgs.append(msg)

        flags = 0
        if dp.ofproto.OFP_VERSION == ofproto_v1_0.OFP_VERSION:
            flags = dp.ofproto.OFPSF_REPLY_MORE
        elif dp.ofproto.OFP_VERSION == ofproto_v1_2.OFP_VERSION:
            flags = dp.ofproto.OFPSF_REPLY_MORE
        elif dp.ofproto.OFP_VERSION == ofproto_v1_3.OFP_VERSION:
            flags = dp.ofproto.OFPMPF_REPLY_MORE

        if msg.flags & flags:
            return
        del self.waiters[dp.id][msg.xid]
        lock.set()

    @set_ev_cls([ofp_event.EventOFPSwitchFeatures], MAIN_DISPATCHER)
    def features_reply_handler(self, ev):
        msg = ev.msg
        dp = msg.datapath

        if dp.id not in self.waiters:
            return
        if msg.xid not in self.waiters[dp.id]:
            return
        lock, msgs = self.waiters[dp.id][msg.xid]
        msgs.append(msg)

        del self.waiters[dp.id][msg.xid]
        lock.set()

##########

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        dpid = datapath.id
        self.macTable[dpid] = {}

        mod_flow_entry(datapath, {}, ofproto.OFPFC_DELETE)
        self.logger.debug("switch %s joined!" % dpid)

        time.sleep(10)

        # ARP packets -> Go to Table1
        match = parser.OFPMatch(eth_type=ether.ETH_TYPE_ARP)
        inst = [parser.OFPInstructionGotoTable(1)]
        self.add_flow(datapath, 0, 100, match, inst)

        # Packet In for ARP packets (Table1 & Table2)
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        self.add_flow(datapath, 1, 10, match, inst)
        self.add_flow(datapath, 2, 10, match, inst)

    def add_flow(self, datapath, table_id, priority, match, inst):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        mod = parser.OFPFlowMod(datapath=datapath, table_id=table_id, priority=priority, match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        port = msg.match['in_port']

        pkt = packet.Packet(data=msg.data)
        pkt_ethernet = pkt.get_protocol(ethernet.ethernet)

        if not pkt_ethernet:
            return

        pkt_arp = pkt.get_protocol(arp.arp)

        if pkt_arp:
            if pkt_arp.opcode == arp.ARP_REQUEST:
                self.recv_request(datapath, port, pkt, pkt_ethernet, pkt_arp)
            elif pkt_arp.opcode == arp.ARP_REPLY:
                self.recv_reply(datapath, port, pkt, pkt_arp)

    def recv_request(self, datapath, port, pkt, pkt_ethernet, pkt_arp):
        dpid = datapath.id
        ofproto = datapath.ofproto

        print "dpid=%d (in_port=%d) recv ARP request" % (dpid, port)
        print "src_ip: %s, dst_ip: %s" % (pkt_arp.src_ip, pkt_arp.dst_ip)

        if pkt_arp.dst_ip in self.arpTable:
            self.send_reply(datapath, port, pkt_ethernet, pkt_arp)
        else:
            self.learn_MAC(pkt_arp, datapath, port)
            self._send_request(datapath, port, ofproto.OFPP_ALL, pkt)
            print "FLOODING"

    def send_reply(self, datapath, port, pkt_ethernet, pkt_arp):
        self.logger.debug("----------------------------------")
        self.logger.debug("pkt_arp.dst_ip=%s" % pkt_arp.dst_ip)
        self.logger.debug("----------------------------------")
        target_mac = self.arpTable[pkt_arp.dst_ip]
        in_port=self.macTable[datapath.id][pkt_arp.src_mac]
        
        pkt = packet.Packet()
        pkt.add_protocol(ethernet.ethernet(ethertype=pkt_ethernet.ethertype, dst=pkt_ethernet.src, src=target_mac))
        pkt.add_protocol(arp.arp(opcode=arp.ARP_REPLY, src_mac=target_mac, src_ip=pkt_arp.dst_ip, dst_mac=pkt_arp.src_mac, dst_ip=pkt_arp.src_ip))
        self._send_reply(datapath, in_port, port, pkt)
        
        print "%d (%d) send ARP reply by CONTROLLER" % (datapath.id, port)
        print "src_ip: %s, dst_ip: %s" % (pkt_arp.dst_ip, pkt_arp.src_ip)

    def recv_reply(self, datapath, port, pkt, pkt_arp):
        dpid = datapath.id
        ofproto = datapath.ofproto
	parser = datapath.ofproto_parser
	
        print "datapath=%d (in_port=%d) recv ARP reply" % (dpid, port)
        print "src_ip: %s, dst_ip: %s" % (pkt_arp.src_ip, pkt_arp.dst_ip)

        self.learn_MAC(pkt_arp, datapath, port)
        
        out_port = self.macTable[dpid][pkt_arp.dst_mac]
        self._send_reply(datapath, port, out_port, pkt)
        
        print "datapath=%d (out_@prt=%d) send ARP reply" % (dpid, out_port)
        print "src_ip: %s, dst_ip: %s" % (pkt_arp.src_ip, pkt_arp.dst_ip)

        match = parser.OFPMatch(eth_type=ether.ETH_TYPE_ARP, arp_op=arp.ARP_REPLY, arp_tpa=pkt_arp.dst_ip)
        actions = [parser.OFPActionOutput(out_port, ofproto.OFPCML_NO_BUFFER)]
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        self.add_flow(datapath, 1, 200, match, inst)
        self.logger.info("-------------------------")
        self.logger.info("---Reply flow mod!!!!!---")
        self.logger.info("-------------------------")

    def learn_MAC(self, pkt_arp, datapath, port):
        dpid = datapath.id
        if pkt_arp.src_ip not in self.arpTable:
            self.arpTable[pkt_arp.src_ip] = pkt_arp.src_mac

        if pkt_arp.src_mac not in self.macTable[dpid]:
            self.macTable[dpid][pkt_arp.src_mac] = port
            
        self.setTable_src(datapath, pkt_arp.src_mac, pkt_arp.src_ip)
        self.setTable_dst(datapath, pkt_arp.src_mac, pkt_arp.src_ip, self.macTable[dpid][pkt_arp.src_mac])
        
        print self.arpTable
    
    def setTable_src(self, datapath, src_mac, src_ip):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        match = parser.OFPMatch(eth_type=ether.ETH_TYPE_ARP, arp_op=arp.ARP_REQUEST, arp_tpa=src_ip)
        
        set_src = parser.OFPActionSetField(eth_src=src_mac)
        set_op = parser.OFPActionSetField(arp_op=arp.ARP_REPLY)
        set_spa = parser.OFPActionSetField(arp_spa=src_ip)
        set_sha = parser.OFPActionSetField(arp_sha=src_mac)
        
        actions = [set_src, set_op, set_spa, set_sha]
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_WRITE_ACTIONS, actions), parser.OFPInstructionGotoTable(2)]
        self.add_flow(datapath, 1, 100, match, inst)
        
    def setTable_dst(self, datapath, dst_mac, dst_ip, port):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        match = parser.OFPMatch(eth_type=ether.ETH_TYPE_ARP, arp_op=arp.ARP_REQUEST, arp_spa=dst_ip)
        
        set_dst = parser.OFPActionSetField(eth_dst=dst_mac)
        set_tpa = parser.OFPActionSetField(arp_tpa=dst_ip)
        set_tha = parser.OFPActionSetField(arp_tha=dst_mac)
        
        output = parser.OFPActionOutput(port)
        
        actions = [set_dst, set_tpa, set_tha, output]
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_WRITE_ACTIONS, actions)] 
        self.add_flow(datapath, 2, 100, match, inst)      
    
    def _send_request(self, datapath, in_port, port, pkt):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        
        pkt.serialize()
        
        actions = [parser.OFPActionOutput(port=port)]
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=ofproto.OFP_NO_BUFFER, in_port=in_port, actions=actions, data=pkt.data)
        datapath.send_msg(out)

    def _send_reply(self, datapath, in_port, port, pkt):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        pkt.serialize()
    
        actions = [parser.OFPActionOutput(port=port)]
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=ofproto.OFP_NO_BUFFER, in_port=ofproto.OFPP_CONTROLLER, actions=actions, data=pkt.data)
