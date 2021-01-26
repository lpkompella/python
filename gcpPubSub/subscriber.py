#!/usr/bin/env python


import argparse


def list_subscriptions_in_topic(project_id, topic_id):
    """Lists all subscriptions for a given topic."""
    # [START pubsub_list_topic_subscriptions]
    from google.cloud import pubsub_v1

    # TODO(developer)
    # project_id = "your-project-id"
    # topic_id = "your-topic-id"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    response = publisher.list_topic_subscriptions(request={"topic": topic_path})
    for subscription in response:
        print(subscription)
    # [END pubsub_list_topic_subscriptions]


def list_subscriptions_in_project(project_id):
    """Lists all subscriptions in the current project."""
    # [START pubsub_list_subscriptions]
    from google.cloud import pubsub_v1

    # TODO(developer)
    # project_id = "your-project-id"

    subscriber = pubsub_v1.SubscriberClient()
    project_path = f"projects/{project_id}"

    # Wrap the subscriber in a 'with' block to automatically call close() to
    # close the underlying gRPC channel when done.
    with subscriber:
        for subscription in subscriber.list_subscriptions(
            request={"project": project_path}
        ):
            print(subscription.name)
    # [END pubsub_list_subscriptions]


def create_subscription(project_id, topic_id, subscription_id):
    """Create a new pull subscription on the given topic."""
    # [START pubsub_create_pull_subscription]
    from google.cloud import pubsub_v1

    # TODO(developer)
    # project_id = "your-project-id"
    # topic_id = "your-topic-id"
    # subscription_id = "your-subscription-id"

    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    # Wrap the subscriber in a 'with' block to automatically call close() to
    # close the underlying gRPC channel when done.
    with subscriber:
        subscription = subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic_path}
        )

    print(f"Subscription created: {subscription}")
    # [END pubsub_create_pull_subscription]


def receive_messages(project_id, subscription_id, timeout=None):
    """Receives messages from a pull subscription."""
    # [START pubsub_subscriber_async_pull]
    # [START pubsub_quickstart_subscriber]
    from concurrent.futures import TimeoutError
    from google.cloud import pubsub_v1

    # TODO(developer)
    # project_id = "your-project-id"
    # subscription_id = "your-subscription-id"
    # Number of seconds the subscriber should listen for messages
    # timeout = 5.0

    subscriber = pubsub_v1.SubscriberClient()
    # The `subscription_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/subscriptions/{subscription_id}`
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message):
        print(f"Received {message}.")
        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()
    # [END pubsub_subscriber_async_pull]
    # [END pubsub_quickstart_subscriber]


def synchronous_pull(project_id, subscription_id):
    """Pulling messages synchronously."""
    # [START pubsub_subscriber_sync_pull]
    from google.api_core import retry
    from google.cloud import pubsub_v1

    # TODO(developer)
    # project_id = "your-project-id"
    # subscription_id = "your-subscription-id"

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    NUM_MESSAGES = 10

    # Wrap the subscriber in a 'with' block to automatically call close() to
    # close the underlying gRPC channel when done.
    with subscriber:
        # The subscriber pulls a specific number of messages. The actual
        # number of messages pulled may be smaller than max_messages.
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": NUM_MESSAGES},
            retry=retry.Retry(deadline=800),
        )

        ack_ids = []
        for received_message in response.received_messages:
            print(f"Received: {received_message.message.data}.")
            ack_ids.append(received_message.ack_id)

        # Acknowledges the received messages so they will not be sent again.
        subscriber.acknowledge(
            request={"subscription": subscription_path, "ack_ids": ack_ids}
        )

        print(
            f"Received and acknowledged {len(response.received_messages)} messages from {subscription_path}."
        )
    # [END pubsub_subscriber_sync_pull]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("project_id", help="Your Google Cloud project ID")

    subparsers = parser.add_subparsers(dest="command")
    list_in_topic_parser = subparsers.add_parser(
        "list-in-topic", help=list_subscriptions_in_topic.__doc__
    )
    list_in_topic_parser.add_argument("topic_id")

    list_in_project_parser = subparsers.add_parser(
        "list-in-project", help=list_subscriptions_in_project.__doc__
    )

    create_parser = subparsers.add_parser("create", help=create_subscription.__doc__)
    create_parser.add_argument("topic_id")
    create_parser.add_argument("subscription_id")


    receive_parser = subparsers.add_parser("receive", help=receive_messages.__doc__)
    receive_parser.add_argument("subscription_id")
    receive_parser.add_argument("timeout", default=None, type=float, nargs="?")


    synchronous_pull_parser = subparsers.add_parser(
        "receive-synchronously", help=synchronous_pull.__doc__
    )
    synchronous_pull_parser.add_argument("subscription_id")


    args = parser.parse_args()

    if args.command == "list-in-topic":
        list_subscriptions_in_topic(args.project_id, args.topic_id)
    elif args.command == "list-in-project":
        list_subscriptions_in_project(args.project_id)
    elif args.command == "create":
        create_subscription(args.project_id, args.topic_id, args.subscription_id)
    elif args.command == "receive":
        receive_messages(args.project_id, args.subscription_id, args.timeout)
    elif args.command == "receive-synchronously":
        synchronous_pull(args.project_id, args.subscription_id)
