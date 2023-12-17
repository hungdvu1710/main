import pika, json
import os
from main import Product, db
from dotenv import load_dotenv

load_dotenv()

params = pika.URLParameters(os.environ.get("CLOUDAMQP_URL"))

connection = pika.BlockingConnection(params)

channel = connection.channel()

channel.queue_declare(queue="main")


def callback(ch, method, properties, body):
    print("Received in main ne")

    print(properties.content_type)

    if properties.content_type == "product_created":
        data = json.loads(body)
        print(data)
        print("in here")
        product = Product(id=data["id"], title=data["title"], image=data["image"])
        db.session.add(product)
        db.session.commit()

    elif properties.content_type == "product_updated":
        data = json.loads(body)
        print(data)
        product = Product.query.get(data["id"])
        product.title = data["title"]
        product.image = data["image"]
        db.session.commit()

    elif properties.content_type == "product_deleted":
        data = json.loads(body)
        print(data)
        product = Product.query.get(data)
        db.session.delete(product)
        db.session.commit()


channel.basic_consume(queue="main", on_message_callback=callback, auto_ack=True)

print("Started Consuming")

channel.start_consuming()

channel.close()
