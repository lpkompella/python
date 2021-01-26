#!/usr/bin/env python

import argparse

def publish_messages(project_id, topic_id):
    """Publishes multiple messages to a Pub/Sub topic."""
    # [START pubsub_quickstart_publisher]
    # [START pubsub_publish]
    from google.cloud import pubsub_v1

    # TODO(developer)
    # project_id = "your-project-id"
    # topic_id = "your-topic-id"

    publisher = pubsub_v1.PublisherClient()

    topic_path = publisher.topic_path(project_id, topic_id)

    for n in range(1, 10):
        data = "Message number {}".format(n)
        # Data must be a bytestring
        data = data.encode("utf-8")
        # When you publish a message, the client returns a future.
        future = publisher.publish(topic_path, data)
        print(future.result())

    print(f"Published messages to {topic_path}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("project_id", help="Google Cloud project ID")

    subparsers = parser.add_subparsers(dest="command")

    publish_parser = subparsers.add_parser("publish", help=publish_messages.__doc__)
    publish_parser.add_argument("topic_id")

    args = parser.parse_args()

    if args.command == "publish":
        publish_messages(args.project_id, args.topic_id)
