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
        vals = message.get('vals') # dict

        data={}
        fields = self.env['hr.employee'].fields_get()
        for field in fields.keys():
            if field in vals.keys():
                data.update({field: vals[field]})


        if nip and vals:
            exist = self.env['hr.employee'].search([('nip','=',nip)])
            if exist:
                if data:
                    kv=[]
                    sql = "update hr_employee"
                    sql += " set "
                    for key in data.keys():
                        kv.append(f"{key}='{data[key]}'")
                    sql += ", ".join(kv)
                    sql += f" where nip='{nip}'"

                    self.env.cr.execute(sql)
                    _logger.info('updated')
            else:
                # data = vals
                # data.update({'nip':nip, 'name':name})
                # self.env['hr.employee'].create(data)
                self.employee_created(message)


    # 16 consumer
    def employee_created(self, message):
        _logger.info('**************************** create employee ********************')

        # _logger.info(fields.keys())
        # _logger.info(message) #json
        nip = message.get('nip')
        name = message.get('name')
        vals = message.get('vals') 
        # _logger.info(vals)
        
        data = {}
        fields = self.env['hr.employee'].fields_get()
        for field in fields.keys():
            if field in vals.keys():
                data.update({field: vals[field]})

        data.update({"name":name, "nip":nip})

        # _logger.info(data) #json
        if 'message_attachment_count' in data:
            del data['message_attachment_count']
        if 'message_follower_ids' in data:
            del data['message_follower_ids']
        if 'resource_id' in data:
            del data['resource_id']
        if 'category_ids' in data:
            del data['category_ids']
        if '__last_update' in data:
            del data['__last_update']

        # _logger.info(data)

        exist = self.env['hr.employee'].search([('nip','=',nip)])
        if not exist:
            self.env['hr.employee'].create(data)
            _logger.info('created')
        else:
            _logger.info(f'empoloyee {name} exists...')

    def write(self, vals):
        res = super(Employee, self).write(vals)
        _logger.info(vals)

        # send data to kafka
        topic = "employee13_updated"
        producer = self.get_producer(topic)
        for x in self:
            data = self.format_outgoing_fields(vals)
            producer.send(topic, value={
                "nip":  x.nip, 
                "name": x.name, 
                "vals": data} )
            producer.flush()
        
        return res 
    
    """
    format field many2one supaya di penerima muncul id dan name
    """    
    def format_outgoing_fields(self, vals):
        data = vals
        fields = self.env['hr.employee'].fields_get()
        for field_name in fields.keys():
            if field_name in vals.keys():
                field = fields[field_name]
                field_type=field['type']
                if field_type=='many2one':
                    id = vals[field_name]
                    rel = field['relation']
                    data[field_name]={
                        "type":field['type'], 
                        "relation": rel
                        "value":{
                            "id": id, 
                            "name": self.env[rel].browse(id)['name'],
                        }}
                else:
                    data[field_name]={
                        "type": field_type,
                        "value": vals[field_name]
                    }

        _logger.info(data)
        return data

    @api.model
    def create(self, vals):
        res = super(Employee, self).create(vals)
        _logger.info(vals)

        # send data to kafka

        topic = "employee13_created"
        producer = self.get_producer(topic)
        for x in res:
            producer.send(topic, value={
                "nip": x.nip, 
                "name": x.name, 
                "vals":vals} )
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

    def action_test_create(self):
        data = {'active': True, 'address_home_id': False, 'country_id': False, 'gender': False, 'marital': 'single', 'spouse_complete_name': False, 'spouse_birthdate': False, 'children': 0, 'place_of_birth': False, 'country_of_birth': False, 'birthday': False, 'identification_id': False, 'passport_id': False, 'bank_account_id': False, 'permit_no': False, 'visa_no': False, 'visa_expire': False, 'certificate': 'other', 'study_field': False, 'study_school': False, 'emergency_contact': False, 'emergency_phone': False, 'km_home_work': 0, 'image_1920': False, 'barcode': False, 'pin': False, 'departure_description': False, 'nip': 'A9', 'department_id': False, 'job_id': False, 'job_title': False, 'company_id': 1, 'address_id': 1, 'work_phone': '+62 21 8516290', 'mobile_phone': False, 'work_email': False, 'resource_calendar_id': 1, 'parent_id': False, 'coach_id': False, 'name': 'Abi A9'}
        self.env['hr.employee'].create(data)