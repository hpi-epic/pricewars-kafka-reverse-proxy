import argparse
import json
import time
import hashlib
import base64

import pandas as pd
from flask import Flask, send_from_directory, request
from kafka import KafkaConsumer
from kafka import TopicPartition
from kafka.errors import NoBrokersAvailable

class KafkaReverseProxy:
    def __init__(self, kafka_endpoint: str):
        self.app = Flask(__name__)
        self.kafka_endpoint = kafka_endpoint
        self.register_routes()
        # The following kafka topics are accessible by merchants
        self.topics = ['buyOffer', 'marketSituation']

    def register_routes(self):
        self.app.add_url_rule('/export/data/<path:topic>', 'export_csv_for_topic', self.export_csv_for_topic,
                              methods=['GET'])
        self.app.add_url_rule('/topics', 'get_topics', self.get_topics, methods=['GET'])
        self.app.add_url_rule('/data/<path:path>', 'static_proxy', self.static_proxy, methods=['GET'])

    def export_csv_for_topic(self, topic):
        auth_header = request.headers.get('Authorization')
        merchant_token = auth_header.split(' ')[-1] if auth_header else None
        merchant_id = calculate_id(merchant_token) if merchant_token else None

        if topic not in self.topics:
            return json.dumps({'error': 'unknown topic'})

        try:
            consumer = KafkaConsumer(consumer_timeout_ms=1000, bootstrap_servers=self.kafka_endpoint)
            topic_partition = TopicPartition(topic, 0)
            consumer.assign([topic_partition])

            consumer.seek_to_beginning()
            start_offset = consumer.position(topic_partition)

            consumer.seek_to_end()
            end_offset = consumer.position(topic_partition)

            msgs = []
            '''
            Assumption: message offsets are continuous.
            Start and end can be anywhere, end - start needs to match the amount of messages.
            TODO: when deletion of some individual messages is possible and used, refactor!
            '''
            max_messages = 10 ** 5
            offset = max(start_offset, end_offset - max_messages)
            consumer.seek(topic_partition, offset)
            for msg in consumer:
                '''
                Don't handle steadily incoming new messages
                only iterate to last messages when requested
                '''
                if offset >= end_offset:
                    break
                offset += 1
                try:
                    msg_json = json.loads(msg.value.decode('utf-8'))
                    # filtering on messages that can be filtered on merchant_id
                    if 'merchant_id' not in msg_json or msg_json['merchant_id'] == merchant_id:
                        msgs.append(msg_json)
                except ValueError as e:
                    print('ValueError', e, 'in message:\n', msg.value)
            consumer.close()

            if topic == 'marketSituation':
                df = market_situation_shaper(msgs)
            else:
                df = pd.DataFrame(msgs)

            filename = topic + '_' + str(int(time.time()))
            filepath = 'data/' + filename + '.csv'
            df.to_csv(filepath, index=False)
            response = {'url': filepath}
        except Exception as e:
            response = {'error': 'failed with: ' + str(e)}

        return json.dumps(response)

    @staticmethod
    def get_topics():
        return json.dumps(self.topics)

    @staticmethod
    def static_proxy(path):
        return send_from_directory('data', path, as_attachment=True)


def market_situation_shaper(list_of_msgs):
    """
        Returns pd.DataFrame Table with columns:
            timestamp
            merchant_id
            product_id

            quality
            price
            prime
            shipping_time_prime
            shipping_time_standard
            amount
            offer_id
            uid
    """
    # snapshot timestamp needs to be injected into the offer object
    # also the triggering merchant
    expanded_offers = []
    for situation in list_of_msgs:
        for offer in situation['offers']:
            offer['timestamp'] = situation['timestamp']
            if 'merchant_id' in situation:
                offer['triggering_merchant_id'] = situation['merchant_id']
            expanded_offers.append(offer)
    return pd.DataFrame(expanded_offers)


def calculate_id(token: str) -> str:
    return base64.b64encode(hashlib.sha256(token.encode('utf-8')).digest()).decode('utf-8')


def parse_arguments():
    parser = argparse.ArgumentParser(description='Kafka Reverse Proxy')
    parser.add_argument('--port', type=int, default=8001, help='port to bind server to')
    parser.add_argument('--kafka_url', type=str, required=True, help='Endpoint of the kafka bootstrap server')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_arguments()
    server = KafkaReverseProxy(args.kafka_url)
    server.app.run(host='0.0.0.0', port=args.port)
