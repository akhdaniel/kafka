# -*- coding: utf-8 -*-

from odoo import fields, models, api, _
from odoo.exceptions import UserError, ValidationError
from kafka import KafkaProducer
from json import dumps

import logging
_logger = logging.getLogger(__name__)


    
class Employee(models.Model):
    _inherit = 'hr.employee'

    # 16 consumer
    def employee_updated(self, message):
        _logger.info('**************************** modify employee ********************')
        _logger.info(message) #json
        # self.env['hr.employee'].write( message )

    # 16 consumer
    def employee_created(self, message):
        _logger.info('**************************** create employee ********************')
        _logger.info(message) #json
        # self.env['hr.employee'].write( message )


    def write(self, vals):
        res = super(Employee, self).write(vals)

        # send data to kafka
        topic = "employee13_updated"
        producerRecord = self.env['kafka.master.consumer'].search([('active', '=', True), ('name', '=', topic)], limit=1)

        producer = KafkaProducer(bootstrap_servers=eval(producerRecord.host),
                        value_serializer=lambda x: dumps(x).encode('utf-8'))
        for x in self:
            producer.send(topic, value={"id": x.id, "vals":vals} )
            producer.flush()
        
        return res 
    

    def init(self):
        topics = [
            "employee13_created",
            "employee13_updated",
            "employee13_deleted",
        ]
        _logger.info(f'initialize topics for v13...{topics}')
        producerRecord = self.env['kafka.master.consumer'].search([('active', '=', True), ('name', '=', 'template')], limit=1)

        producer = KafkaProducer(bootstrap_servers=eval(producerRecord.host or "['147.139.134.170:9093']"),
                        value_serializer=lambda x: dumps(x).encode('utf-8'))
        for topic in topics:
            producer.send(topic, value={} )
            producer.flush()
        

                
