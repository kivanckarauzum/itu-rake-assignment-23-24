import json  # for converting from string to list
import socket  # for communicating with the broker
import argparse  # for parsing the program argument(s)

# Communication constants
BROKER_HOST = "127.0.0.1"
BROKER_PORT = 8000
BUFFER_SIZE = 4096

# Message constants
ACK = "ACK"
FORMAT = "utf-8"
SEPERATOR = "$"
TOPIC = None


def __generate_subscribe_message(topic_name: str = "default"):
    return SEPERATOR.join(["SUB", topic_name])


def __string_to_array(string_data: str):
    return json.loads(string_data)


if __name__ == "__main__":
    """
        Description:
            - aim is to write a subscriber client, which communicates in UDP
            - the client subscribes to a topic in the broker, then receives ACK
            - afterwards, the client listens the topic forever

        Attention:
            - a subscriber client can only be subscribed to a single topic
            - topic name is given as a program argument
            
        Sample Use:
            python sub.py -topic=image_processor
    """
    # write your code here
    parser = argparse.ArgumentParser(description='Subscriber Client')
    parser.add_argument('-topic', type=str, help='Specify the topic to subscribe')
    args = parser.parse_args()
    if args.topic:
        TOPIC = args.topic
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        try:
            udp_socket.connect((BROKER_HOST, BROKER_PORT))
            subscribe_message = __generate_subscribe_message(TOPIC)

            udp_socket.send(subscribe_message.encode(FORMAT))
            print(f"Subscribed to the topic: {TOPIC}")

            ack = udp_socket.recv(BUFFER_SIZE).decode(FORMAT)
            if ack == ACK:
                print("Received ACK from the broker.")
            else:
                print("Failed to receive proper ACK from the broker.")
                exit()


            while True:
                data = udp_socket.recv(BUFFER_SIZE).decode(FORMAT)
                data_array = __string_to_array(data)
                print(f"Received data: {data_array}")

        except Exception as e:
            print(f"Error: {e}")

        finally:
            udp_socket.close()
    else:
        print("Please specify the topic using the -topic argument.")