import pika, os, logging
import callback_methods as cm

CHECK_IN_EVENT_QUEUE = 'checkin_events'
CHECK_OUT_EVENT_QUEUE = 'checkout_events'
RATING_EVENT_QUEUE = 'rating_events'
QR_SCAN_EVENT_QUEUE = 'qr_scan_events'

print 'Starting Preprocessor Script'

# Setup pika with CloudAMQP settings
print 'Configuring pika'
logging.basicConfig()
url = os.environ.get('CLOUDAMQP_URL', 'amqp://khtvlijq:k46AzRMT-ynVS6vP3zPAwciJ6LFcyeMI@owl.rmq.cloudamqp.com:5672/khtvlijq')
params = pika.URLParameters(url)


# Connect to CloudAMQP
print 'Connecting to CloudAMQP'
connection = pika.BlockingConnection(params)
channel = connection.channel()

# Bind CloudAMQP queues to consumers
print 'Binding CloudAMQP queues to consumers'
channel.queue_declare(queue=CHECK_IN_EVENT_QUEUE, durable=True)
channel.basic_consume(cm.handle_checkin, queue=CHECK_IN_EVENT_QUEUE, no_ack=True)

channel.queue_declare(queue=CHECK_OUT_EVENT_QUEUE, durable=True)
channel.basic_consume(cm.handle_checkout, queue=CHECK_OUT_EVENT_QUEUE, no_ack=True)

channel.queue_declare(queue=RATING_EVENT_QUEUE, durable=True)
channel.basic_consume(cm.handle_rating, queue=RATING_EVENT_QUEUE, no_ack=True)

channel.queue_declare(queue=QR_SCAN_EVENT_QUEUE, durable=True)
channel.basic_consume(cm.handle_qr_scan, queue=QR_SCAN_EVENT_QUEUE, no_ack=True)

# Handle incoming events
print 'Starting consuming messages'
channel.start_consuming()