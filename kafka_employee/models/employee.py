# -*- coding: utf-8 -*-

from odoo import fields, models, api, _
from odoo.exceptions import UserError, ValidationError
from kafka import KafkaProducer
from json import dumps

import logging
_logger = logging.getLogger(__name__)


    
class Employee(models.Model):
    _inherit = 'hr.employee'

    
    nip = fields.Char(
        string='NIP',
        required=True
    )
    
    #  consumer from 16
    def employee_updated(self, message):
        _logger.info('**************************** modify employee ********************')
        _logger.info(message) #json

        name = message.get('name') 
        nip = message.get('nip') 
        vals = message.get('vals') 
        # self.env['hr.employee'].sudo().browse(id).write( vals )

        if nip and vals:
            exist = self.env['hr.employee'].search([('nip','=',nip)])
            if exist:
                kv=[]
                sql = "update hr_employee"
                sql += " set "
                for key in vals.keys():
                    kv.append(f"{key}='{vals[key]}'")
                sql += ", ".join(kv)
                sql += f" where nip='{nip}'"

                self.env.cr.execute(sql)
            else:
                data = vals
                data.update({'nip':nip, 'name':name})
                self.env['hr.employee'].create(data)


    # 16 consumer
    def employee_created(self, message):
        _logger.info('**************************** create employee ********************')

        fields = self.env['hr.employee'].fields_get()
        _logger.info(fields.keys())
        # _logger.info(message) #json
        nip = message.get('nip')
        name = message.get('name')
        vals = message.get('vals') 
        
        data = {}
        for field in fields.keys():
            if field in vals[0]:
                data.update({field: vals[0][field]})
                
        # _logger.info(data) #json
        if 'message_attachment_count' in data:
            del data['message_attachment_count']
        if 'message_follower_ids' in data:
            del data['message_follower_ids']

        if data:
            exist = self.env['hr.employee'].search([('nip','=',nip)])
            if not exist:
                self.env['hr.employee'].create(data)
            else:
                _logger.info(f'empoloyee {name} exists...')

    def write(self, vals):
        res = super(Employee, self).write(vals)

        # send data to kafka
        topic = "employee13_updated"
        producer = self.get_producer(topic)
        for x in self:
            data = vals[0]
            del data['message_attachment_count']
            del data['message_follower_ids']
            producer.send(topic, value={
                "nip": x.nip, 
                "name": x.name, 
                "vals":data} )
            producer.flush()
        
        return res 
    
    
    @api.model_create_multi
    def create(self, vals):
        res = super(Employee, self).create(vals)

        # send data to kafka

        topic = "employee13_created"
        producer = self.get_producer(topic)
        for x in res:
            producer.send(topic, value={"nip": x.nip, "name": x.name, "vals":vals} )
            producer.flush()

        return res
        
    def get_producer(self, topic):
        producerRecord = self.env['kafka.master.consumer'].search([('name', '=', topic)], limit=1)

        producer = KafkaProducer(bootstrap_servers=eval(producerRecord.host),
                        value_serializer=lambda x: dumps(x).encode('utf-8'))        
        
        return producer
    
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
        

                
