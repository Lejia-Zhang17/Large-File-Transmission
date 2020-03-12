from socket import *
import struct
import hashlib
import os
from tqdm import tqdm

# This is the side of sender
# Send File
blocksize = 5000


def send_file():

    server_name = '127.0.0.1'  # This is the IP address of local Internet environment

    server_port = 12000
    clientsocket = socket(AF_INET, SOCK_DGRAM)

    file_path = '/Users/z/Desktop/Client_AS1/concert.MOV'  # Self-define the path of the file
    remaining, round_times, header_data = header_info(file_path)

    # Sender: 1st communication: send header data of file to server
    clientsocket.sendto(header_data, (server_name, server_port))

    print("Sender: ready to divide the file")

    label = 0  # Set original pointer at start place of file
    for i in range(round_times):
        while True:
            f = open(file_path, 'rb')
            f.seek(label, 0)
            segment1 = f.read(blocksize)
            segment2 = f.read(blocksize)
            segment3 = f.read(blocksize)

        #  pack the file fragment & send the packets
            udp_fragment1 = encapsulate_udp(segment1, blocksize)
            udp_fragment2 = encapsulate_udp(segment2, blocksize)
            udp_fragment3 = encapsulate_udp(segment3, blocksize)
        #   Sender: 2nd communication : send UDP fragment
            clientsocket.sendto(udp_fragment1, (server_name, server_port))
            clientsocket.sendto(udp_fragment2, (server_name, server_port))
            clientsocket.sendto(udp_fragment3, (server_name, server_port))

        # Sender: 3rd communication: receive ack and decode it
            ack1, server_address = clientsocket.recvfrom(20480)
            ack2, server_address = clientsocket.recvfrom(20480)
            ack3, server_address = clientsocket.recvfrom(20480)

        # check the status of ack, back to the start of the transfer loop  and redo if ack="No"
            if ack1.decode() and ack2.decode() and ack3.decode() == "Yes":
                label = f.tell()  # modify the location of the pointer for transferring remaining
                f.close()
                break

    # send remaining part
    for v in range(remaining):
        while True:
            f = open(file_path, 'rb')
            f.seek(label, 0)
            segment = f.read(blocksize)

            # Get the size of the remaining part
            size = f.tell()-label

            #  pack the file fragment & send the packets
            udp_fragment = encapsulate_udp(segment, size)

            # Sender: 4th communication : send UDP fragment
            clientsocket.sendto(udp_fragment, (server_name, server_port))
            label = f.tell()
            f.close()

            # Sender: 5th communication: receive ack and decode it
            ack, server_address = clientsocket.recvfrom(20480)

            if ack.decode() == 'Yes':
                break


# attain the header information of the file
def header_info(file_path):
    global blocksize
    file_size = os.path.getsize(file_path)
    round_times, remaining = calculate_round_times(file_size)
    segment_size = blocksize + 73  # 73 = md5(16*4) + size for Q format (8) + size for bracers of list (1)
    file_md5 = get_md5(file_path)

    header_data = [blocksize, file_size, round_times, remaining, segment_size, file_md5]
    data = struct.pack('!QQQQQ', header_data[0], header_data[1], header_data[2], header_data[3], header_data[4])\
        + header_data[5].encode()
    return remaining, round_times, data


# calculate the times of round based on pipeline pattern
def calculate_round_times(file_size):
    global blocksize
    times = int(file_size / blocksize)

    if file_size == times * blocksize:
        round_time = int(times/3)-1
        remaining = times - (3*round_time)
        print("This is roundtime",round_time)
        return round_time, remaining
    else:
        times = times + 1
        round_time = int(times / 3) - 1
        remaining = times - (3 * round_time)
        print("This is roundtime", round_time)
        return round_time, remaining


# encapsulate each file segment into a UDP fragment
def encapsulate_udp(file_segment, length):
    global blocksize
    md5 = hashlib.md5(file_segment).hexdigest()
    udp_fragment = struct.pack('!Q', length) + file_segment + md5.encode()
    return udp_fragment


# get md5 of the file as checksum
def get_md5(filename):
    f = open(filename, 'rb')
    contents = f.read()
    f.close()
    return hashlib.md5(contents).hexdigest()


def main():
    send_file()


if __name__ == '__main__':
    main()






