from flask import Flask, request, jsonify
from flask_restx import Api, Namespace
from util.kafka_client import KafkaClient
import whisper
import threading
import logging

app = Flask(__name__)
api = Api(app, version='1.0', title='STT API', description='Handles speech-to-text conversion')
ns = Namespace('api', description='STT operations')
api.add_namespace(ns)

kafka_client = KafkaClient('localhost:9092')
consumer = kafka_client.create_consumer('audio-topic', 'stt-service-group')
model = whisper.load_model('base')

# Configure logging settings
logging.basicConfig(level=logging.INFO)

def consume_audio():
    for msg in consumer:
        audio_data = msg.value
        correlation_id = dict(msg.headers).get(b'correlation_id', b'').decode('utf-8')
        
        # Process the audio data
        logging.info("Processing audio data...")
        audio = whisper.load_audio_from_bytes(audio_data)
        audio = whisper.pad_or_trim(audio)
        mel = whisper.log_mel_spectrogram(audio).to(model.device)

        _, probs = model.detect_language(mel)
        logging.info(f"Detected language: {max(probs, key=probs.get)}")

        options = whisper.DecodingOptions()
        result = whisper.decode(model, mel, options)

        logging.info("Text result: %s", result.text)

        # Send the text result back to Kafka
        headers = [('correlation_id', correlation_id.encode('utf-8'))]
        kafka_client.producer.send('text-topic', value=result.text.encode('utf-8'), headers=headers)
        kafka_client.producer.flush()

consumer_thread = threading.Thread(target=consume_audio)
consumer_thread.daemon = True
consumer_thread.start()

if __name__ == '__main__':
    app.run(port=5001, debug=True)
