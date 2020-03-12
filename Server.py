from socket import *
import struct
import hashlib
from tqdm import tqdm
# This is the side of receiver


def receive_message():
    server_port = 12000
    server_socket = socket(AF_INET, SOCK_DGRAM)
    server_socket.bind(('', server_port))
    print('The server is ready to receive')

    # Receiver: 1st communication : receive header message
    header_message, client_address = server_socket.recvfrom(20480)
    blocksize, file_size, round_times, remaining, segment_size = struct.unpack('!QQQQQ', header_message[:40])
    file_md5 = header_message[40:].decode()
    print("blocksize", blocksize)
    print("file_size", file_size)
    print("round_times", round_times)
    print("remaining", remaining)
    print("segment_size", segment_size)
    print("file_md5", file_md5)

    # Define the location and the file name of the received file
    f = open("/Users/z/Desktop/Server_AS1/concert-received.MOV", 'wb')
    for i in tqdm(range(round_times)): # use tqdm to see the progress of the transmission
        while True:
            # Receiver: 2nd communication : receive UDP fragment
            udp_fragment1, client_address = server_socket.recvfrom(segment_size)
            udp_fragment2, client_address = server_socket.recvfrom(segment_size)
            udp_fragment3, client_address = server_socket.recvfrom(segment_size)

            file_length, segment1, segment1_checksum = parse_file(udp_fragment1)
            file_length, segment2, segment2_checksum = parse_file(udp_fragment2)
            file_length, segment3, segment3_checksum = parse_file(udp_fragment3)

            # get MD5 as ack
            md51 = get_md5(segment1)
            md52 = get_md5(segment2)
            md53 = get_md5(segment3)

            # compare the sent md5 and md5 analysed by receiver
            ack1, result1 = checksum_check(md51, segment1_checksum)
            ack2, result2 = checksum_check(md52, segment2_checksum)
            ack3, result3 = checksum_check(md53, segment3_checksum)

            if result1 and result2 and result3 is True:
                f.write(segment1)
                f.write(segment2)
                f.write(segment3)

                # Receiver: 3rd communication: send ack
                server_socket.sendto(ack1, client_address)
                server_socket.sendto(ack2, client_address)
                server_socket.sendto(ack3, client_address)
                break

            else:
                server_socket.sendto(ack1, client_address)
                server_socket.sendto(ack2, client_address)
                server_socket.sendto(ack3, client_address)

    # Get the remaining part of the file
    for v in range(remaining):
        while True:
            # Receiver: 4th communication : receive UDP fragment
            udp_fragment, client_address = server_socket.recvfrom(segment_size)
            file_length, segment, segment_checksum = parse_file(udp_fragment)
            md5 = get_md5(segment)
            ack, result = checksum_check(md5, segment_checksum)
            # Receiver: 5th communication: send ack
            server_socket.sendto(ack, client_address)

            if result is True:
                f.write(segment)
                break

    f.close()


# Get length, segment, checksum of the file
def parse_file(udp_fragment):
    file_length = struct.unpack('!Q', udp_fragment[:8])[0]
    file_segment = udp_fragment[8:8+file_length]
    segment_checksum = udp_fragment[8+file_length:].decode()
    return file_length, file_segment, segment_checksum


# Get the checksum(MD5) of file
def get_md5(segment):
    return hashlib.md5(segment).hexdigest()


# Compare the sent md5 and md5 analysed by receiver
def checksum_check(md5, received_md5):
    if md5 == received_md5:
        return "Yes".encode(), True
    else:
        return "No".encode(), False


def main():
    receive_message()


if __name__ == '__main__':
    main()