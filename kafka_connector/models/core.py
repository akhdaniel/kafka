# -*- coding: utf-8 -*-

from odoo import api, SUPERUSER_ID, _
from odoo.exceptions import ValidationError
from kafka import KafkaConsumer, TopicPartition
import logging
import json
import threading
import time

import odoo

# Run timeout connection
TIMEOUT = 10

_logger = logging.getLogger(__name__)


class RunKafka(object):
    db_name = odoo.tools.config['db_name']
    registries = odoo.modules.registry.Registry.registries

    if not db_name:
        for db_name, registry in registries.d.items():
            db_name = db_name

    registry = odoo.registry(db_name)

    new_cr = registry.cursor()
    new_cr._cnx.autocommit = True
    env = api.Environment(new_cr, SUPERUSER_ID, {})

    topic = ''
    seq = ''

    def __init__(self):
        # self.start()
        pass

    def loop(self, topic=None, seq=None):
        with self.registry.cursor() as new_cr:
            new_env = api.Environment(new_cr, SUPERUSER_ID, {})
            master_consumer = new_env['kafka.master.consumer'].search(
                [('active', '=', True), ('name', '=', topic)], limit=1)
            new_env.cr.commit()

            if master_consumer:
                host = master_consumer.host
                params = master_consumer.params
                group = master_consumer.group
                topic = master_consumer.name
                model = master_consumer.model_name.model
                function = master_consumer.function
                poll_timeout = master_consumer.poll_timeout
                fetch_interval = master_consumer.fetch_interval

                consumer = eval(
                    'KafkaConsumer(bootstrap_servers=' + host + ',' + params + ',' + 'group_id=' + "'" + group + "'" + ')')
                topics = consumer.topics()
                if master_consumer.name in topics:
                    pass
                else:
                    master_consumer.running = False
                    raise ValidationError(_('Topic not found!'))
                # consumer.subscribe(topic or master_consumer.group or False)
                _logger.info("KAFKA: Listening topic [%s] is active" % topic)

                consumer.subscribe(topic)
                while True:
                    consumer_poll = consumer.poll(timeout_ms=poll_timeout)
                    master_consumer = new_env['kafka.master.consumer'].search(
                        [('active', '=', True), ('name', '=', topic)], limit=1)
                    new_env.cr.commit()
                    if master_consumer and not master_consumer.running:
                        consumer.unsubscribe()
                        consumer.close(autocommit=False)
                        raise ValidationError(_('Kafka stopped by Button STOP!'))

                    if len(consumer_poll) < 1:
                        time.sleep(fetch_interval)
                        continue
                    else:
                        for tp, messages in consumer_poll.items():
                            for message in messages:
                                # more check, if master change after this looping
                                # so master_consumer will get new data
                                master_consumer = new_env['kafka.master.consumer'].search(
                                    [('active', '=', True), ('name', '=', topic)], limit=1)
                                new_env.cr.commit()

                                vals = message.value
                                f = 'new_env["' + model + '"].' + function + '(%s)' % vals
                                try:
                                    eval(f)
                                    new_env.cr.commit()
                                    consumer.commit()
                                except Exception as e:
                                    new_env.cr.commit()
                                    _logger.info(
                                        "KAFKA %s EXCEPTION ==> %s" % (seq, 'Cannot execute function [%s], makesure '
                                                                            'you have correct parameters according in '
                                                                            'this master consumer topic %s. '
                                                                            'Error message: %s' % (function, topic, e)))
                                    if master_consumer.commit_on_error:
                                        if master_consumer.stop_on_error:
                                            # if stop when error True, kafka will close connection and commit
                                            # current offset
                                            consumer.close(autocommit=True)
                                            self.send_notification(model_name=master_consumer._name,
                                                                   res_id=master_consumer.id, error=e)

                                            raise ValidationError(_('%s' % e))
                                        else:
                                            # keep kafka run, and commit current offset
                                            continue
                                    else:
                                        # Kafka will not commit current offset, and will retry till func execution
                                        # success and offset committed
                                        if master_consumer.stop_on_error:
                                            # Kafka will stop and no commit current offset
                                            consumer.unsubscribe()
                                            consumer.close(autocommit=False)
                                            self.send_notification(model_name=master_consumer._name,
                                                                   res_id=master_consumer.id, error=e)
                                            self.env.cr.commit()
                                            raise ValidationError(_('%s' % e))
                                        else:
                                            # if nothing checklist, kafka will continue and retry uncommitted offset
                                            consumer.unsubscribe()
                                            time.sleep(10)
                                            consumer.subscribe(topic)
                                            continue

                                _logger.info("KAFKA %s RETURN ==> %s" % (seq, message))
            else:
                raise ValidationError(_('No data found in Master Consumer!'))

    def run(self):
        topic = self.topic
        seq = self.seq
        while True:
            try:
                self.loop(topic=topic, seq=seq)
            except Exception as e:
                with self.registry.cursor() as new_cr:
                    new_env = api.Environment(new_cr, SUPERUSER_ID, {})
                    consumer = new_env['kafka.master.consumer'].search(
                        [('active', '=', True), ('name', '=', topic)], limit=1)
                    consumer.running = False
                    consumer.state = 'stop'
                    new_env.cr.commit()
                    _logger.info("KAFKA TOPIC: %s connection stopped. Info: %s" % (topic, e))
                    break

    def start(self):
        master_consumer = self.env['kafka.master.consumer'].search([('active', '=', True), ('name', '=', self.topic)])
        thread = None
        if master_consumer:
            if self.topic:
                self.seq = 'TOPIC [%s]' % self.topic

                # Activate thread mode
                self.t = threading.Thread(name="Kafka-%s-%s" % (__name__, self.topic), target=self.run)
                self.t.daemon = True
                self.t.start()
                master_consumer.thread = self.t
                thread = self.t
            else:
                _logger.exception("Cannot get Topic, please specified one!")
                raise ValidationError(_("Cannot get Topic, please specified one!"))
        else:
            _logger.exception(
                "Cannot get Master Consumer, please create one!")
            raise ValidationError(_("Cannot get Master Consumer, please create one!"))

        return thread

    def start_kafka(self, topic=None):
        self.topic = topic
        res = self.start()
        return res

    def send_notification(self, model_name=None, res_id=None, error=None):
        with self.registry.cursor() as cr1:
            env1 = api.Environment(cr1, SUPERUSER_ID, {})
            group_administrator = env1['ir.model.data']._xmlid_to_res_id('base.group_erp_manager')
            user_administrators = self.get_users_from_group(group_administrator)
            users = env1['res.users'].search([('id', 'in', user_administrators)])
            if users:
                notification_ids = [(0, 0,
                                     {
                                         'res_partner_id': user.partner_id.id,
                                         'notification_type': 'inbox'
                                     }
                                     ) for user in users]
                env1['mail.message'].create({
                    'message_type': "notification",
                    'body': "Kafka suddenly stopped due to error: %s" % error,
                    'subject': "Kafka Stopped",
                    'partner_ids': [(4, user.partner_id.id) for user in users],
                    'model': model_name,
                    'res_id': res_id,
                    'notification_ids': notification_ids,
                })
            env1.cr.commit()

    def get_users_from_group(self, group_id):
        with self.registry.cursor() as cr2:
            env2 = api.Environment(cr2, SUPERUSER_ID, {})
            users_ids = []
            sql_query = """SELECT uid FROM res_groups_users_rel WHERE gid = %s"""
            params = (group_id,)
            env2.cr.execute(sql_query, params)
            results = env2.cr.fetchall()
            for users_id in results:
                users_ids.append(users_id[0])
            env2.cr.commit()
            return users_ids
