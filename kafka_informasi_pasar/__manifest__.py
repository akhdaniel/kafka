
{
    'name': "Sync Informasi Pasar With Kafka",

    'summary': """
        Sync Informasi Pasar Kafka
    """,
    "category": "Utilities",
    'author': "Brantas Abipraya",
    'website': "http://www.brantas-abipraya.co.id",
    'version': '0.1',
    'license': 'LGPL-3',

    'depends': [
        'mrk_project_parameter',
        'kafka_connector'
    ],

    'data': [
        'data/consumer.xml',
    ],
}