from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json

app = Flask(__name__)

# Configure Kafka producer
producer = KafkaProducer(
    bootstrap_servers='my-cluster-kafka-bootstrap:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/produce', methods=['POST'])
def produce_message():
    try:
        content = request.json
        topic = content['topic']
        message = content['message']
        
        # Send message to Kafka
        producer.send(topic, message)
        producer.flush()
        
        return jsonify({'status': 'Message sent to Kafka'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
