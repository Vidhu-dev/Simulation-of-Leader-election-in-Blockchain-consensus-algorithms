import random
import hashlib
import time
import threading
import csv


class Block:
    def __init__(self, index, previous_hash, transactions, stake, miner_id):
        self.index = index
        self.previous_hash = previous_hash
        self.transactions = transactions
        self.stake = stake
        self.miner_id = miner_id
        self.hash = self.calculate_hash()

    def calculate_hash(self):
        block_string = f"{self.index}{self.previous_hash}{self.transactions}{self.stake}"
        return hashlib.sha256(block_string.encode()).hexdigest()


class Node:
    def __init__(self, node_id, coin, age):
        self.node_id = node_id
        self.blockchain = []  # The blockchain ledger for this node
        self.neighbors = set()  # Set of neighboring nodes
        self.coin = coin
        self.age = age
        self.voter = 0
        self.tempBlock = 0
        self.coinage = self.coin * self.age
        self.stake = 0
        self.blockMined = 0

    def calculateStake(self):
        percent = random.randint(0, 100)  # Percent of the coin node wants to put on stake
        self.stake = self.coinage * percent / 100
        return self.stake

    def add_neighbor(self, neighbor):
        self.neighbors.add(neighbor)

    def broadcast_block(self, message):
        # time.sleep(0.03)
        for neighbor in self.neighbors:
            neighbor.receive_block(message)

    def receive_block(self, message):
        # print(f"block recieved by block id {self.node_id}")
        self.tempBlock = message
        if self.verify_pos(message):
            # print(f"Node {self.node_id} added block {message.index} to its blockchain.")
            self.broadcast_vote(True)
            self.voter += 1
            if self.tempBlock != 0 and self.voter > 5 / 2:  # Check for majority
                # print(f"node id : {self.node_id} adding the block to blockchain  ")
                self.blockchain.append(self.tempBlock)
                self.tempBlock = 0
                # print(self.blockchain)
        else:
            # print(f"Node {self.node_id} received block {message.index}, but PoW verification failed.")
            self.broadcast_vote(False)
            # print(self.blockchain)

    def broadcast_vote(self, vote):
        # time.sleep(0.01)
        # print("sending vote to neighbor")
        for neighbor in self.neighbors:
            neighbor.receive_vote(vote)

    def receive_vote(self, vote):

        if vote:
            self.voter += 1
        # print(
        #     f"Number of votes received by node id {self.node_id} in favour of block: {self.voter}  and status of temp block : {self.tempBlock}")
        if self.tempBlock != 0 and self.voter > 5 / 2:  # Check for majority
            # print(f"node id : {self.node_id} adding the block to blockchain  ")
            self.blockchain.append(self.tempBlock)
            self.tempBlock = 0
            # print(self.blockchain)

    def print_blockchain(self):
        print(
            f"block id: {self.node_id} blockchain ledger {self.blockchain} balance of node: {self.coin} total coinage: {self.coinage}")

    def mine_block(self):
        index = len(self.blockchain)
        previous_hash = self.blockchain[-1].hash if self.blockchain else "0"
        transactions = self.generate_dummy_transactions()
        stake = self.stake
        new_block = Block(index, previous_hash, transactions, stake, self.node_id)
        new_block = self.proof_of_stake(new_block)
        self.save_mine_info(new_block.index, new_block.miner_id)

        self.blockchain.append(new_block)
        self.blockMined += 1
        # print(self.blockchain)
        # time.sleep(0.3)
        self.broadcast_block(new_block)

    def save_mine_info(self, block_number, miner_id):
        csv_file_path = 'mine_data_pos.csv'
        with open(csv_file_path, 'a', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow([block_number, miner_id])
        print(f'Block {block_number} mined by Node {miner_id} has been added to {csv_file_path}')

    def proof_of_stake(self, block):
        self.coinage = self.coinage - self.stake
        block.hash = block.calculate_hash()
        return block

    def generate_dummy_transactions(self):
        num_transactions = random.randint(1, 5)
        transactions = [f"Transaction {i}" for i in range(num_transactions)]
        return transactions

    def verify_pos(self, block):
        new_block = Block(block.index, block.previous_hash, block.transactions, block.stake, block.miner_id)
        expected_hash = new_block.calculate_hash()
        return expected_hash == block.hash


def mine_blocks(node, blocks_to_mine):
    for _ in range(blocks_to_mine):
        node.mine_block()


def data_to_csv():
    global nodes
    csv_file_path = 'node_data_pos.csv'
    field_names = ['Node ID', 'Block Mined']
    with open(csv_file_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)

        # Write the header row
        csv_writer.writerow(field_names)

        # Write data for each node
        for node in nodes:
            csv_writer.writerow([node.node_id, node.blockMined])
    print(f'Data has been written to {csv_file_path}')


def initialize_csv():
    csv_file_path = 'mine_data_pos.csv'
    field_names = ['Block Number', 'Miner ID']

    # Create an empty CSV file or open the existing one
    with open(csv_file_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)

        # Write the header row
        csv_writer.writerow(field_names)


def write_metrics_to_csv(algorithm):
    global throughput, latency, energy_consumption, fault_tolerance
    field_names = ['Algorithm', 'Throughput', 'Latency', 'Energy Consumption', 'Fault Tolerance']
    filename = "algorithm_stats.csv"
    with open(filename, 'a', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        # Write the metrics data
        csv_writer.writerow([algorithm, throughput, latency, energy_consumption, fault_tolerance])


if __name__ == "__main__":
    # Create multiple nodes
    num_nodes = 1000
    nodes = [Node(node_id=i, coin=random.randint(100, 1000), age=random.randint(1, 90)) for i in range(num_nodes)]
    initialize_csv()

    # Define network topology
    for i in range(num_nodes):
        for j in range(num_nodes):
            if i != j:
                nodes[i].add_neighbor(nodes[j])

    blocks_to_mine = 25
    k = 0
    STAKE_ARRAY = [0] * num_nodes
    start_time = time.time()
    while blocks_to_mine > k:
        for i in range(num_nodes):
            STAKE_ARRAY[i] = nodes[i].calculateStake()

        MAX_STAKE = max(STAKE_ARRAY)
        SELECTED_NODE = STAKE_ARRAY.index(MAX_STAKE)
        # for i in range(num_nodes):
        #     nodes[i].print_blockchain()
        print(f"Maximum stake stake found {MAX_STAKE} and the node {SELECTED_NODE} is putting the stake")
        print(STAKE_ARRAY)
        # threads = []
        # thread = threading.Thread(target=mine_blocks, args=(nodes[SELECTED_NODE], blocks_to_mine))
        # threads.append(thread)
        # thread.start()
        nodes[SELECTED_NODE].mine_block()

        for i in range(num_nodes):
            nodes[i].print_blockchain()

        # for thread in threads:
        #     thread.join()

        for i in range(num_nodes):
            nodes[i].print_blockchain()

        print(f"*************************************Block {k} is mined******************************************")
        k += 1
    end_time = time.time()
    # Calculate the elapsed time
    elapsed_time = end_time - start_time

    print(f"Elapsed time: {elapsed_time:.4f} seconds")
    print(f"Number of blocks generated {blocks_to_mine}")
    throughput = blocks_to_mine / elapsed_time
    latency = elapsed_time / blocks_to_mine
    print(f"Throughtput: {throughput}")
    print(f"Latency {latency}")

    data_to_csv()
    energy_consumption = 10
    fault_tolerance = 0
    write_metrics_to_csv("POS")
