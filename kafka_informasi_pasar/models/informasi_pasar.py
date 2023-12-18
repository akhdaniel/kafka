# -*- coding: utf-8 -*-

from odoo import fields, models, api, _
from odoo.exceptions import UserError, ValidationError
from kafka import KafkaProducer
from json import dumps

import logging
_logger = logging.getLogger(__name__)


    
class mrk_project_parameter(models.Model):
    _inherit = 'mrk.project_parameter'
    
    def write(self, vals):
        res = super(mrk_project_parameter, self).write(vals)
        _logger.info(vals)
        topic = "informasi_pasar13_created"
        producer = self.get_producer(topic)
        for x in self:
            producer.send(topic, value={
                "sequents": x.sequents, 
                "name": x.name,
                "partner_name": x.partner_id.name or False,
                "sistem_kontrak":x.sistem_kontrak.name or False,
                "pagu_anggaran":x.planned_revenue,
                "harga_perkiraan":x.hps,
                "bobot": x.persen_bobot_infopasar,
                "kesimpulan": x.string_bobot_infopasar,
                "state":dict(self._fields['state'].selection).get(x.state),} )
            producer.flush()
        
        return res 
    

    @api.model
    def create(self, vals):
        res = super(mrk_project_parameter, self).create(vals)
        _logger.info(vals)

        # send data to kafka
        topic = "informasi_pasar13_updated"
        producer = self.get_producer(topic)
        for x in res:
            producer.send(topic, value={
                "sequents": x.sequents, 
                "name": x.name,
                "partner_name": x.partner_id.name or False,
                "sistem_kontrak":x.sistem_kontrak.name or False,
                "pagu_anggaran":x.planned_revenue,
                "harga_perkiraan":x.hps,
                "bobot": x.persen_bobot_infopasar,
                "kesimpulan": x.string_bobot_infopasar,
                "state":dict(self._fields['state'].selection).get(x.state)})
            producer.flush()

        return res
        
    def get_producer(self, topic):
        producerRecord = self.env['kafka.master.consumer'].search([('name', '=', topic)], limit=1)

        producer = KafkaProducer(bootstrap_servers=eval(producerRecord.host),
                        value_serializer=lambda x: dumps(x).encode('utf-8'))        
        
        return producer
    
    def init(self):
        topics = [
            "informasi_pasar13_created",
            "informasi_pasar13_updated",
            "informasi_pasar13_deleted",
        ]
        _logger.info(f'initialize topics for v13...{topics}')
        producerRecord = self.env['kafka.master.consumer'].search([('active', '=', True), ('name', '=', 'template')], limit=1)

        producer = KafkaProducer(bootstrap_servers=eval(producerRecord.host or "['147.139.134.170:9093']"),
                        value_serializer=lambda x: dumps(x).encode('utf-8'))
        for topic in topics:
            producer.send(topic, value={} )
            producer.flush()