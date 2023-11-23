# -*- coding: utf-8 -*-
{
    'name': "Kafka Connector",

    'summary': """
        Create multiple KafkaConsumer topics and run simultaneously, while sending messages to predefined functions 
        in JSON format.
    """,

    'description': """
        This module can integrate Odoo with Apache Kafka using subscribe topics. 
        So if there a new message from Kafka server, Odoo will fetch message immediately.
    """,

    'author': 'Afifi A.M.',
    'maintainer': 'Afifi A.M.',
    'category': 'Tools',
    'license': 'OPL-1',
    'version': '16.0.1.2',

    'depends': [
        'base',
        'mail'
    ],
    'data': [
        'security/ir.model.access.csv',
        'views/kafka_master_consumer_views.xml',
        'data/ir_actions_server.xml',
        'data/scheduler.xml',
    ],
    'images': ['static/description/ThumbnailBanner.gif'],
    'external_dependencies': {
        'python': ['kafka-python'],
    },
    'installable': True,
    'auto_install': False,
    'application': False,
    'price': 100,
    'currency': 'EUR',
}
