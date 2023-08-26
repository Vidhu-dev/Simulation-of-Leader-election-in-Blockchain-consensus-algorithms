import random
import pandas as pd
import hashlib
import time
import threading
import os, sys
import csv
# Setting up lock for future use
lock = threading.Lock()
minerSelectedCondition = threading.Condition()
arrayNotEmptyCondition = threading.Condition()
highRoundCondition = threading.Condition()

MINE_LOG = {}


# Function to output on console
def displayBlock(block):
    tnx = ""
    for eachTransaction in block.transaction:
        tnx += f'\t\t[{eachTransaction.FROM}->{eachTransaction.TO}, Amount:{eachTransaction.AMOUNT}, Fee:{eachTransaction.FEE}]\n'
    print(
        f'\nBlock Id\t: {block.id}\nNounce\t\t: {block.nounce}\nTimestamp\t: {block.timestamp}\nTransactions:\n {tnx}CurrentHash\t: {block.current_hash}\nPreviousHash\t: {block.previous_hash}\nMinerName\t: {block.miner_name}\nMinerId\t\t:{block.miner_id}')


# Dataframe to array convertor
def dataFrameToArrayofTransactions(transactions):
    toBeReturned = []
    for i in transactions.index:
        toBeReturned.append(
            Transaction(
                transactions["From"][i],
                transactions["To"][i],
                transactions["Amount"][i],
                transactions["Fee"][i]
            )
        )
    return toBeReturned


# Sliding window of current transactions
def updateCurrentTxns():
    global START, END, CURRENT_TXN
    START = END
    END = END + TXN_THRESHOLD - 1
    CURRENT_TXN = dataFrameToArrayofTransactions(pd.read_csv("transactions.csv")[START:END + 1])


# Preparation before hashing
def preHashComputation(nounce, timestamp, previoushash, minerid, transactions, c_round):
    string_representation = f'{nounce}{c_round}{timestamp}{previoushash}{minerid}'
    for x in transactions:
        string_representation += f'{x.FROM}{x.TO}{x.AMOUNT}{x.FEE}'
    return string_representation


# Compute the hash
def computeHash(text):
    return hashlib.sha512(text.encode("utf-8")).hexdigest()


def verify(miner_name):
    global VERIFICATIONS, TARGET

    if len(CURRENT_BLOCKCHAIN) > 0:
        block = CURRENT_BLOCKCHAIN[-1]
        expected_hash = computeHash(
            preHashComputation(block.nounce, block.timestamp, block.previous_hash, block.miner_id, block.transaction))

        if expected_hash == block.current_hash and block.current_hash[:len(TARGET)] == TARGET:
            VERIFICATIONS += 1
            print(f"Block is verified by {miner_name}")


# Function to output mine log
def printMineLog():
    print("\nMine Report:")
    for each in MINE_LOG:
        print(
            f"\n\tMiner's Id: {each}\n\t\tMiner's Name: {MINE_LOG[each]['name']}\n\t\tBlocks Mined: {MINE_LOG[each]['count']}")
        print()


# Function to generate the report after run
def generateReport():
    report = open("report.csv", "w")
    report.write("block_id,nounce,timestamp,from,to,amount,fee,current_hash,previous_hash,miner_name,miner_id\n")
    for each in CURRENT_BLOCKCHAIN:
        for txn in each.transaction:
            report.write(
                f"{each.id},{each.nounce},{each.timestamp},{txn.FROM},{txn.TO},{txn.AMOUNT},{txn.FEE},{each.current_hash},{each.previous_hash},{each.miner_name},{each.miner_id}\n")
    report.close()


# Function to prepare the run
def prepareToRun():
    if os.path.exists("transactions.csv"):
        os.remove("transactions.csv")
    if os.path.exists("report.csv"):
        os.remove("report.csv")


# Class of Miner
class Miner(threading.Thread):
    def __init__(self, id, name):
        threading.Thread.__init__(self)
        self.id = id
        self.name = name
        self.cround = 1
        global MINE_LOG
        try:
            MINE_LOG[self.id]
        except KeyError:
            MINE_LOG[self.id] = {"name": self.name, "count": 0}

    def run(self):
        global TXN_THRESHOLD, CURRENT_TXN, CURRENT_BLOCKCHAIN, CURRENT_BLOCK_ID, TARGET, minerSelected, start, highest_round

        with minerSelectedCondition:
            while minerSelected != -1:
                print(f"{self.name} is waiting for completing this round")
                self.cround = 1
                minerSelectedCondition.wait()

            timestamp_used = time.time()
            nounce_used = 1

            if len(CURRENT_BLOCKCHAIN) == 0:
                previous_hash = "0" * 128
            else:
                previous_hash = CURRENT_BLOCKCHAIN[-1].current_hash

            current_hash = computeHash(
                preHashComputation(nounce_used, timestamp_used, previous_hash, self.id, CURRENT_TXN, self.cround))

            while self.cround < ROUNDS_COUNT:
                with highRoundCondition:
                    if highest_round - self.cround >= 5:
                        print(f"{self.name} exiting from mining")
                        self.cround = 1
                        minerSelectedCondition.wait()

                nounce_used += 1
                timestamp_used = time.time()
                current_hash = computeHash(
                    preHashComputation(nounce_used, timestamp_used, previous_hash, self.id, CURRENT_TXN, self.cround))

                if current_hash[:len(TARGET)] == TARGET:
                    print(f"{self.cround} completed by {self.name}")
                    if highest_round < self.cround:
                        highest_round = self.cround
                        print(f"{self.name} is at highest round {highest_round}")

                    self.cround += 1

        if ROUNDS_COUNT == self.cround:
            minerSelected = self.id
            print(f'{self.name} selcted as miner\n {current_hash}')

        with lock:
            newblock = Block(
                CURRENT_BLOCK_ID,
                nounce_used,
                timestamp_used,
                CURRENT_TXN,
                current_hash,
                previous_hash,
                self.name,
                self.id
            )

            CURRENT_BLOCK_ID += 1

            updateCurrentTxns()
            CURRENT_BLOCKCHAIN.append(newblock)
            end = time.time()
            C_TIME.append(end - start)
            save_mine_info(newblock.id, newblock.miner_id)
            MINE_LOG[self.id]["count"] += 1
            MINE_LOG[self.id]["block"] = CURRENT_BLOCKCHAIN[-1]
            displayBlock(CURRENT_BLOCKCHAIN[-1])
            print()
            print(f"Consensus Round {len(CURRENT_BLOCKCHAIN)} took {end - start} time")
            print(C_TIME)

            with minerSelectedCondition:
                minerSelected = -1
                minerSelectedCondition.notify_all()

            with highRoundCondition:
                highest_round = 0
                highRoundCondition.notify_all()

            with arrayNotEmptyCondition:
                miners.append(Miner(self.id, self.name))
                arrayNotEmptyCondition.notify_all()
                print('Starting new consensus round---------------------')


# Class of Transaction
class Transaction:
    def __init__(self, FROM, TO, AMOUNT, FEE):
        self.FROM = FROM
        self.TO = TO
        self.AMOUNT = AMOUNT
        self.FEE = FEE


# Class of Block
class Block:
    def __init__(self, id, nounce, timestamp, transactions, current_hash, previous_hash, miner_name, miner_id):
        self.id = id
        self.nounce = nounce
        self.timestamp = timestamp
        self.transaction = transactions
        self.current_hash = current_hash
        self.previous_hash = previous_hash
        self.miner_name = miner_name
        self.miner_id = miner_id


# Class for generating transactions
class DatasetHandler(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        self.txn_file = open("transactions.csv", "a")
        self.running = True

        while self.running:
            buffer = []
            for i in range(8):
                from_field = chr(random.randint(ord("A"), ord("Z")))
                to_field = chr(random.randint(ord("A"), ord("Z")))
                amount = random.randint(1, 10)
                txn_fees = random.uniform(0, 10)
                buffer.append(f'{from_field},{to_field},{amount},{txn_fees}\n')

            lock.acquire()
            self.txn_file.writelines(buffer)
            lock.release()
            done = True

    def exit(self):
        self.running = False
        self.txn_file.close()


def save_mine_info(block_number, miner_id):
    csv_file_path = 'mine_data_cw.csv'
    with open(csv_file_path, 'a', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([block_number, miner_id])
    print(f'Block {block_number} mined by Node {miner_id} has been added to {csv_file_path}')

def initialize_csv():
    csv_file_path = 'mine_data_cw.csv'
    field_names = ['Block Number', 'Miner ID']

    # Create an empty CSV file or open the existing one
    with open(csv_file_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)

        # Write the header row
        csv_writer.writerow(field_names)

# Main
try:
    # Initial Preparation
    prepareToRun()
    initialize_csv()
    # Global Variable Initialisation
    TARGET = "00000"
    CURRENT_BLOCK_ID = 1
    CURRENT_BLOCKCHAIN = []
    TXN_THRESHOLD = 8
    print("Injecting Initial Transactions")

    open("transactions.csv", "a").write(f'From,To,Amount,Fee\n')

    # Starting the Datagenerator Thread
    data_generator_thread = DatasetHandler()
    data_generator_thread.start()
    START = 0
    END = TXN_THRESHOLD - 1
    CURRENT_TXN = dataFrameToArrayofTransactions(pd.read_csv("transactions.csv")[START:END + 1])

    minerSelected = -1
    MINER_COUNT = 1000
    ROUNDS_COUNT = 10
    C_TIME = []

    # Initialising miners
    miners = []
    highest_round = 0

    for i in range(MINER_COUNT):
        miners.append(Miner(id=i + 1, name=f'Miner-{i + 1}'))

    start = time.time()

    for i in range(0, len(miners)):
        miners[i].start()

    miners = []

    while True:
        with arrayNotEmptyCondition:
            while not miners:
                arrayNotEmptyCondition.wait()

            start = time.time()

            for i in range(0, len(miners)):
                miners[i].start()

            miners = []



# Error Handling
except PermissionError:
    print(
        "One of the system files is being used by some other process. Please release the file by shutting down other processes.")
    sys.exit()

except:
    time.sleep(2)
    data_generator_thread.exit()
    minerSelected = -1
    printMineLog()
    generateReport()
    print("Process Completed")
    sys.exit()
