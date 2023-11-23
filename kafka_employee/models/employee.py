# -*- coding: utf-8 -*-

from odoo import fields, models, api, _
from odoo.exceptions import UserError, ValidationError
from kafka import KafkaProducer
from json import dumps

import logging
_logger = logging.getLogger(__name__)


    
class Employee(models.Model):
    _inherit = 'hr.employee'

    # consumer
    def modify_employee(self, message):
        _logger.info('**************************** modify employee ********************')
        _logger.info(message) #json
        # self.env['hr.employee'].write( message )
        _logger.info('**************************** modify employee ********************')


    def write(self, vals):
        res = super(Employee, self).write(vals)

        # send data to kafka
        topic = "employee13-updated"
        producerRecord = self.env['kafka.master.consumer'].search([('active', '=', True), ('name', '=', topic)], limit=1)

        producer = KafkaProducer(bootstrap_servers=eval(producerRecord.host),
                        value_serializer=lambda x: dumps(x).encode('utf-8'))
        for x in self:
            producer.send(topic, value=vals)
            producer.flush()
        
        return res 

                
