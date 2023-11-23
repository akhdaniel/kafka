# -*- coding: utf-8 -*-

from odoo import fields, models, _
from odoo.exceptions import ValidationError
from kafka import KafkaConsumer, TopicPartition
import threading
import json
from odoo.addons.kafka_connector.models import core
import logging

_logger = logging.getLogger(__name__)


class KafkaMasterConsumer(models.Model):
    _name = 'kafka.master.consumer'
    _inherit = ['mail.thread']
    _description = 'Kafka Consumer'

    PARAMS = """auto_offset_reset='earliest',
enable_auto_commit=True,
value_deserializer=lambda m: json.loads(m.decode('ascii'))"""

    active = fields.Boolean(string='Active', default=True, copy=False)
    name = fields.Char('Topic', copy=False)
    group = fields.Char('Group Id', copy=False)
    model_name = fields.Many2one(comodel_name='ir.model', string='Model', index=True)
    function = fields.Char(string='Function', copy=False,
                           help='Enter the function to be executed when getting a message from Kafka, written without parentheses "()".')
    host = fields.Char(string='Kafka Host', default="['localhost:9092']", help='Insert your Kafka Server host.')
    params = fields.Text(string="Parameters", default=PARAMS, help='Add or remove parameter for Kafka connection.')
    poll_timeout = fields.Integer(string='Poll Timeout (ms)', default=1000)
    fetch_interval = fields.Integer(string='Fetch Interval (sec)', default=3,
                                    help='Help to reduce CPU usage, because Kafka consume high CPU when real-time liestening.')

    values = fields.Text(string="Return Values", readonly=1, copy=False)
    commit_on_error = fields.Boolean(string='Commit When Error', default=False, copy=False,
                                     help='Force commit message when an error occurs while the function is executing')
    stop_on_error = fields.Boolean(string='Stop When Error', default=False, copy=False,
                                   help='Stop Kafka when an error occurs while the function is executing')
    run_at_startup = fields.Boolean(string='Run at Startup', default=True, copy=False,
                                    help='Trigger auto run when odoo service started or restart.')

    running = fields.Boolean(string='Running...', default=False, copy=False)
    thread = fields.Char(string='Thread', readonly=1)
    state = fields.Selection(string='', selection=[('running', 'Running'), ('stop', 'Stop')], default='stop', copy=False)

    _sql_constraints = [
        ('topic_uniq', 'unique(name)', 'Topic must be unique!'),
    ]

    def get_values(self):
        for record in self:
            parameters = """
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                consumer_timeout_ms=5000,
                value_deserializer=lambda m: json.loads(m.decode('ascii'))
            """
            try:
                consumer = eval(
                'KafkaConsumer(bootstrap_servers=' + record.host + ',' + parameters + ',' + 'group_id=' + "'kafka-connector-initial-value')")
            except Exception as e:
                raise ValidationError(_('Problem when connecting to Kafka server. Error: %s' % e))

            topics = consumer.topics()
            if record.name in topics:
                for p in consumer.partitions_for_topic(record.name):
                    tp = TopicPartition(record.name, p)
                    consumer.assign([tp])
                    committed = consumer.committed(tp)
                    consumer.seek_to_end(tp)
                    last_offset = consumer.position(tp)
                    offset = last_offset - 1
                    consumer.seek(tp, offset if offset >= 0 else 0)
                vals = None
                for message in consumer:
                    vals = message.value
                    record.values = json.dumps(vals, indent=4)
                    consumer.close(autocommit=False)
                if not record.values:
                    raise ValidationError(_('No data available.'))
            else:
                raise ValidationError(_('No topic available on Kafka Server, please select one existing!'))

    ####################################################################################################################
    ###>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ANOTHER METHOD <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<###
    def run_kafka(self):
        for record in self:
            try:
                record.check_connection()
                thread = core.RunKafka().start_kafka(topic=record.name)
                record.running = True
                record.state = 'running'
                record.thread = thread.name
            except Exception as e:
                raise ValidationError(_('Cannot run Kafka, check Host or Parameters. Error: %s' % e))

    def check_connection(self):
        for record in self:
            try:
                consumer = eval(
                    'KafkaConsumer(bootstrap_servers=' + record.host + ',' + 'group_id=' + "'kafka-connector-initial-value')")
                topics = consumer.topics()
                if record.name in topics:
                    consumer.close(autocommit=False)
                    return True
                else:
                    raise ValidationError(_('Topic not found!'))
            except Exception as e:
                raise

    def stop_kafka(self):
        for record in self:
            record.running = False
            record.state = 'stop'
            record.thread = None

    def action_batch_start_stop(self):
        states = []
        if not self.filtered(lambda consumer: consumer.active):
            raise ValidationError(_('Only active consumer can be Start or Stop.'))

        for rec in self:
            if rec.running:
                states.append(1)
            else:
                states.append(0)
        group = all(val == states[0] for val in states)
        if group:
            pass
        else:
            raise ValidationError(_('Only same state can be Start or Stop, not mixing.'))

        if self.filtered(lambda consumer: consumer.running):
            for consumer in self:
                consumer.stop_kafka()
        else:
            for consumer in self:
                consumer.run_kafka()

    def _register_hook(self):
        super()._register_hook()
        # Reset all running status when odoo first running
        obj = self.env['kafka.master.consumer'].search([('active', '=', True)])
        for record in obj:
            record.running = False
            record.state = 'stop'
            record.thread = None
            if record.run_at_startup:
                # Trigger auto run
                try:
                    record.run_kafka()
                except Exception as e:
                    self.send_notification(res_id=record.id, error=e)
                    continue

    def send_notification(self, res_id=None, error=None):
        group_administrator = self.env['ir.model.data']._xmlid_to_res_id('base.group_erp_manager')
        user_administrators = self.get_users_from_group(group_administrator)
        users = self.env['res.users'].search([('id', 'in', user_administrators)])
        if users:
            notification_ids = [(0, 0,
                                 {
                                     'res_partner_id': user.partner_id.id,
                                     'notification_type': 'inbox'
                                 }
                                 ) for user in users]
            self.env['mail.message'].create({
                'message_type': "notification",
                'body': "System failed to auto run Kafka due to error: %s" % error,
                'subject': "Auto Run Kafka Stopped",
                'partner_ids': [(4, user.partner_id.id) for user in users],
                'model': 'kafka.master.consumer',
                'res_id': res_id,
                'notification_ids': notification_ids,
            })

    def get_users_from_group(self, group_id):
        users_ids = []
        sql_query = """SELECT uid FROM res_groups_users_rel WHERE gid = %s"""
        params = (group_id,)
        self.env.cr.execute(sql_query, params)
        results = self.env.cr.fetchall()
        for users_id in results:
            users_ids.append(users_id[0])
        self.env.cr.commit()
        return users_ids

    def check_kafka_running(self):
        # show running threads
        active_threads = [t.name for t in threading.enumerate()]
        consumer_obj = self.search([('running', '=', True)])
        if consumer_obj:
            for consumer in consumer_obj:
                if consumer.thread not in active_threads:
                    consumer.run_kafka()
                else:
                    continue

        # print("======================== Threads ========================")
        # for t in threading.enumerate():
        #     print(f"- {t.name} | Alive = {t.is_alive()} | Daemon = {t.isDaemon()}")
        # print("============================================================")
