import random
import hashlib
import time

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
        self.is_delegate = False
        self.node_id = node_id
        self.voted_for = None
        self.election_count = 0
        self.blockMined = 0

    def vote_for_delegated_node(self):
        global NUM_NODES
        if self.voted_for is None:
            temp = NUM_NODES
            possible_values = list(range(temp))
            possible_values.remove(self.node_id)
            # print(f"node id {self.node_id} , possible values: {possible_values}")
            chosen_node = random.choice(possible_values)
            self.voted_for = chosen_node
            # print(f"Node {self.node_id} voted for")
            for neighbor in self.neighbors:
                neighbor.increment_election_count(chosen_node)

    def calculateStake(self):
        percent = random.randint(0, 100)  # Percent of the coin node wants to put on stake
        self.stake = self.coinage * percent / 100
        return self.stake

    def increment_election_count(self, id):
        if id == self.node_id:
            self.election_count += 1
            # print(f" Node {self.node_id} as delegated node.")

    def add_neighbor(self, neighbor):
        self.neighbors.add(neighbor)

    def broadcast_block(self, message):
        time.sleep(0.03)
        for neighbor in self.neighbors:
            neighbor.receive_block_delegate(message)

    def broadcast_block_to_delegate(self, message):
        for neighbor in self.neighbors:
            if neighbor.is_delegate:
                neighbor.receive_block_delegate(message)

    def broadcast_block_to_non_delegate(self, message):
        for neighbor in self.neighbors:
            if not neighbor.is_delegate:
                neighbor.receive_block_non_delegate(message)

    def receive_block_non_delegate(self, message):

        self.blockchain.append(message)

    def receive_block_delegate(self, message):

        # print(f"block recieved by block id {self.node_id}")
        self.tempBlock = message
        if self.verify_pos(message):
            # print(f"Node {self.node_id} added block {message.index} to its blockchain.")
            self.broadcast_vote_to_delegate(True)
            self.voter += 1
            if self.tempBlock != 0 and self.voter > 5 / 2:  # Check for majority
                # print(f"node id : {self.node_id} adding the block to blockchain  ")
                self.blockchain.append(self.tempBlock)
                self.tempBlock = 0
                # print(self.blockchain)
        else:
            # print(f"Node {self.node_id} received block {message.index}, but PoW verification failed.")
            self.broadcast_vote_to_delegate(False)
            # print(self.blockchain)

    def broadcast_vote_to_delegate(self, vote):
        time.sleep(0.01)
        for neighbor in self.neighbors:
            if neighbor.is_delegate:
                neighbor.receive_vote_delegate(vote)

    def receive_vote_delegate(self, vote):

        if vote:
            self.voter += 1
        # print(
        #     f"Number of votes received by node id {self.node_id} in favour of block: {self.voter}  and status of temp block : {self.tempBlock}")
        if self.tempBlock != 0 and self.voter > 5 / 2:  # Check for majority
            # print(f"node id : {self.node_id} adding the block to blockchain  ")
            self.blockchain.append(self.tempBlock)
            # self.broadcast_block_to_non_delegate(self.tempBlock)
            self.tempBlock = 0
            # print(self.blockchain)

    def print_blockchain(self):
        print(
            f"block id: {self.node_id} blockchain ledger {self.blockchain} balance of node: {self.coin} total coinage: {self.coinage}")

    def print_node_status(self):
        print(
            f"block id: {self.node_id} vote count {self.election_count} is node delegate: {self.is_delegate}")

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
        print(self.blockchain)
        self.broadcast_block_to_delegate(new_block)
        self.broadcast_block_to_non_delegate(new_block)

    def save_mine_info(self, block_number, miner_id):
        csv_file_path = 'mine_data_dpos200.csv'
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


def selectDelegate():
    global NUM_NODES, STAKE_ARRAY, nodes
    print("************* Voting for delegates .........************")
    for i in range(NUM_NODES):
        nodes[i].vote_for_delegated_node()
    print("************* Selecting delegates .........*************")
    sorted_nodes = sorted(nodes, key=lambda node: node.election_count, reverse=True)
    delegate_node = sorted_nodes[:100]
    print("************* Selected delegates .........************")
    for node in delegate_node:
        print(f"Node ID: {node.node_id}, Vote Count: {node.election_count}")
        node.is_delegate = True

    print("*****************************************************")
    return delegate_node


def data_to_csv():
    global nodes
    csv_file_path = 'node_data_dpos200.csv'
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
    csv_file_path = 'mine_data_dpos200.csv'
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
    with open(filename, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        # Write the metrics data
        csv_writer.writerow(field_names)
        csv_writer.writerow([algorithm, throughput, latency, energy_consumption, fault_tolerance])


if __name__ == "__main__":
    # Create multiple nodes
    NUM_NODES = 200
    nodes = [Node(node_id=i, coin=random.randint(100, 1000), age=random.randint(1, 90)) for i in range(NUM_NODES)]
    initialize_csv()
    # Define network topology
    for i in range(NUM_NODES):
        for j in range(NUM_NODES):
            if i != j:
                nodes[i].add_neighbor(nodes[j])

    blocks_to_mine = 25
    k = 0
    STAKE_ARRAY = [0] * NUM_NODES
    for i in range(NUM_NODES):
        STAKE_ARRAY[i] = nodes[i].calculateStake()
    delegate_nodes = selectDelegate()

    current_delegate_index = 0

    start_time = time.time()
    # pyRAPL.setup()
    # meter = pyRAPL.Measurement('bar')
    # meter.begin()
    while blocks_to_mine > k:
        for i in range(NUM_NODES):
            STAKE_ARRAY[i] = nodes[i].calculateStake()
        delegate_nodes[current_delegate_index].mine_block()
        for i in range(NUM_NODES):
            nodes[i].print_blockchain()
        print(f"*************************************Block {k} is mined******************************************")
        k += 1
        current_delegate_index += 1
        if current_delegate_index == 100:
            current_delegate_index = 0
        print(current_delegate_index)
    # meter.end()
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
    write_metrics_to_csv("DPOS")
