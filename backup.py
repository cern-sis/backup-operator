import kopf
import logging

@kopf.on.create('S3Backup')
def create_fn(body, **kwargs):
    logging.info(f"A handler is called with body: {body}")