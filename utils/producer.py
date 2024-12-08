import time

from confluent_kafka import Producer
import json


class InvoicePublisher:
    def __init__(self):
        self.topic_name = "invoices"
        self.conf = {
            'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': '',
            'sasl.password': '',
            'client.id': 'shiva-laptop'
        }

    def delivery_callback(self, err, msg):
        if err:
            print('Error in publishing the message: {}'.format(err))
        else:
            key = msg.key().decode('utf-8')
            invoice_id = json.loads(msg.value().decode('utf-8'))["InvoiceNumber"]
            print(f"Published message to {self.topic_name} with key = {key} value = {invoice_id}")

    def publish_invoices(self, producer):
        counter = 0
        # This to publish only small number of messages
        counts = 30
        with open("../data/invoices.json") as lines:
            for line in lines:
                invoice = json.loads(line)
                store_id = invoice["StoreID"]
                producer.produce(self.topic_name, key=store_id, value=line, callback=self.delivery_callback)
                time.sleep(0.5)
                producer.poll(1)
                counter = counter + 1
                if counter == counts:
                    break

    def start(self):
        kafka_producer = Producer(self.conf)
        self.publish_invoices(kafka_producer)
        kafka_producer.flush(100)


if __name__ == "__main__":
    publisher = InvoicePublisher()
    publisher.start()
