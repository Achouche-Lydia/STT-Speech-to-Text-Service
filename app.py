from flask import Flask
from flask_restx import Api, Namespace
from util.kafka_client import KafkaClient
import whisper
import threading
import tempfile
import os
import logging  

app = Flask(__name__)
api = Api(app, version='1.0', title='STT API', description='Handles speech-to-text conversion')
ns = Namespace('api', description='STT operations')
api.add_namespace(ns)

kafka_client = KafkaClient('localhost:9092')
producer = kafka_client.create_producer()
model = whisper.load_model('base')


logging.basicConfig(level=logging.INFO)

def consume_audio():
    consumer = kafka_client.create_consumer('audio-topic', 'stt-service-group')
    for msg in consumer:
        audio_data = msg.value
        correlation_id = dict(msg.headers).get(b'correlation_id', b'').decode('utf-8')
        
       
        with tempfile.NamedTemporaryFile(delete=False, suffix='.wav') as temp_audio_file:
            temp_audio_file.write(audio_data)
            temp_audio_file_path = temp_audio_file.name
        
        
        logging.info("Processing audio data...")
        audio = whisper.load_audio(temp_audio_file_path)
        audio = whisper.pad_or_trim(audio)
        mel = whisper.log_mel_spectrogram(audio).to(model.device)

        _, probs = model.detect_language(mel)
        logging.info(f"Detected language: {max(probs, key=probs.get)}")

        options = whisper.DecodingOptions()
        result = whisper.decode(model, mel, options)
        print('lydia1')
        logging.info("Text result: %s", result.text)
       
        os.remove(temp_audio_file_path)

        headers = [('correlation_id', correlation_id.encode('utf-8'))]
        producer.send('text-topic', value=result.text.encode('utf-8'), headers=headers)
        producer.flush()

consumer_thread = threading.Thread(target=consume_audio)
consumer_thread.daemon = True
consumer_thread.start()

if __name__ == '__main__':
    app.run(port=5001, debug=True)
