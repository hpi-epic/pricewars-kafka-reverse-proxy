import argparse
import collections
import csv
import json
import threading
import time
import os
import hashlib
import base64

import pandas as pd
from flask import Flask, send_from_directory, request
from flask_cors import CORS
from flask_socketio import SocketIO, emit
from kafka import KafkaConsumer
from kafka import TopicPartition

app = Flask(__name__, static_url_path='')
CORS(app)
socketio = SocketIO(app)


# The following kafka topics are accessible by merchants and the management UI
topics = ['addOffer', 'buyOffer', 'profit', 'updateOffer', 'updates', 'salesPerMinutes',
          'cumulativeAmountBasedMarketshare', 'cumulativeRevenueBasedMarketshare',
          'marketSituation', 'revenuePerMinute', 'revenuePerHour', 'profitPerMinute']


class KafkaHandler:
    def __init__(self, kafka_endpoint: str):
        self.consumer = KafkaConsumer(bootstrap_servers=kafka_endpoint)
        self.dumps = {}
        end_offset = {}

        for topic in topics:
            self.dumps[topic] = collections.deque(maxlen=100)
            current_partition = TopicPartition(topic, 0)
            self.consumer.assign([current_partition])
            self.consumer.seek_to_end()
            offset = self.consumer.position(current_partition)
            end_offset[topic] = offset > 100 and offset or 100

        topic_partitions = [TopicPartition(topic, 0) for topic in topics]
        self.consumer.assign(topic_partitions)
        for topic in topics:
            self.consumer.seek(TopicPartition(topic, 0), end_offset[topic] - 100)

        self.thread = threading.Thread(target=self.run, args=())
        self.thread.daemon = True  # Demonize thread
        self.thread.start()  # Start the execution

    def run(self):
        for msg in self.consumer:
            try:
                msg_json = json.loads(msg.value.decode('utf-8'))
                if 'http_code' in msg_json and msg_json['http_code'] != 200:
                    continue

                output = {
                    "topic": msg.topic,
                    "timestamp": msg.timestamp,
                    "value": msg_json
                }
                output_json = json.dumps(output)
                self.dumps[str(msg.topic)].append(output)

                socketio.emit(str(msg.topic), output_json, namespace='/')
            except Exception as e:
                print('error emit msg', e)

        self.consumer.close()


class KafkaReverseProxy:
    def __init__(self, kafka_endpoint: str):
        self.kafka_endpoint = kafka_endpoint
        self.kafka_handler = KafkaHandler(kafka_endpoint)

        app.add_url_rule('/status', 'status', self.status, methods=['GET'])
        app.add_url_rule('/export/data/<path:topic>', 'export_csv_for_topic', self.export_csv_for_topic, methods=['GET'])
        app.add_url_rule('/topics', 'get_topics', self.get_topics, methods=['GET'])
        app.add_url_rule('/data/<path:path>', 'static_proxy', self.static_proxy, methods=['GET'])

    @socketio.on('connect')
    def on_connect(self):
        if self.kafka_handler.dumps:
            for msg_topic in self.kafka_handler.dumps:
                messages = list(self.kafka_handler.dumps[msg_topic])
                emit(msg_topic, messages, namespace='/')

    def status(self):
        status_dict = {}
        for topic in self.kafka_handler.dumps:
            status_dict[topic] = {
                'messages': len(self.kafka_handler.dumps[topic]),
                'last_message': self.kafka_handler.dumps[topic][-1] if self.kafka_handler.dumps[topic] else ''
            }
        return json.dumps(status_dict)

    def export_csv_for_topic(self, topic):
        shaper = {
            'marketSituation': market_situation_shaper
        }

        auth_header = request.headers.get('Authorization')
        merchant_token = auth_header.split(' ')[-1] if auth_header else None
        merchant_id = calculate_id(merchant_token) if merchant_token else None

        max_messages = 10 ** 5

        if topic not in topics:
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

            df = shaper[topic](msgs) if topic in shaper else pd.DataFrame(msgs)

            filename = topic + '_' + str(int(time.time()))
            filepath = 'data/' + filename + '.csv'
            df.to_csv(filepath, index=False)
            response = {'url': filepath}
        except Exception as e:
            response = {'error': 'failed with: ' + str(e)}

        return json.dumps(response)

    def get_topics(self):
        return json.dumps(topics)

    def static_proxy(self, path):
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
    parser.add_argument('--port', type=int, default=8001, help='port to bind socketIO App to')
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    # TODO: rename
    kafka_endpoint_test = os.getenv('KAFKA_URL', 'vm-mpws2016hp1-05.eaalab.hpi.uni-potsdam.de:9092')
    KafkaReverseProxy(kafka_endpoint_test)
    socketio.run(app, host='0.0.0.0', port=args.port)
