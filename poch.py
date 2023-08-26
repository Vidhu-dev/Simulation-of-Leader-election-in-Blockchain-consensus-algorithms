import math
import random
import pandas as pd
import hashlib
import time
import threading
import os, sys

# Setting up lock for future use
lock = threading.Lock()
minerSelectedCondition = threading.Condition()
arrayNotEmptyCondition = threading.Condition()

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


# Compute the hash
def computeHash(text):
    return hashlib.sha512(text.encode("utf-8")).hexdigest()


# def verify(miner_name):
#     global VERIFICATIONS, TARGET
#
#     if len(CURRENT_BLOCKCHAIN) > 0:
#         block = CURRENT_BLOCKCHAIN[-1]
#         expected_hash = computeHash(
#             preHashComputation(block.nounce, block.timestamp, block.previous_hash, block.miner_id, block.transaction))
#
#         if expected_hash == block.current_hash and block.current_hash[:len(TARGET)] == TARGET:
#             VERIFICATIONS += 1
#             print(f"Block is verified by {miner_name}")


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
class Miner:
    def __init__(self, id, name):
        self.id = id
        self.name = name
        global MINE_LOG
        try:
            MINE_LOG[self.id]
        except KeyError:
            MINE_LOG[self.id] = {"name": self.name, "count": 0}

    # def run(self):
    #     global TXN_THRESHOLD, CURRENT_TXN, CURRENT_BLOCKCHAIN, CURRENT_BLOCK_ID, TARGET, minerSelected, start
    #
    #     with minerSelectedCondition:
    #         while minerSelected != -1:
    #             print(f"{self.name} is waiting for completing this round")
    #             minerSelectedCondition.wait()
    #
    #         timestamp_used = time.time()
    #         nounce_used = 1
    #
    #         if len(CURRENT_BLOCKCHAIN) == 0:
    #             previous_hash = "0" * 128
    #         else:
    #             previous_hash = CURRENT_BLOCKCHAIN[-1].current_hash
    #
    #         current_hash = computeHash(
    #             preHashComputation(nounce_used, timestamp_used, previous_hash, self.id, CURRENT_TXN))
    #
    #         while current_hash[:len(TARGET)] != TARGET:
    #             nounce_used += 1
    #             timestamp_used = time.time()
    #             current_hash = computeHash(
    #                 preHashComputation(nounce_used, timestamp_used, previous_hash, self.id, CURRENT_TXN))
    #
    #     minerSelected = self.id
    #     print(f'{self.name} selcted as miner\n {current_hash}')
    #
    #     with lock:
    #         newblock = Block(
    #             CURRENT_BLOCK_ID,
    #             nounce_used,
    #             timestamp_used,
    #             CURRENT_TXN,
    #             current_hash,
    #             previous_hash,
    #             self.name,
    #             self.id
    #         )
    #
    #         CURRENT_BLOCK_ID += 1
    #
    #         updateCurrentTxns()
    #         CURRENT_BLOCKCHAIN.append(newblock)
    #         end = time.time()
    #         C_TIME.append(end - start)
    #
    #         MINE_LOG[self.id]["count"] += 1
    #         MINE_LOG[self.id]["block"] = CURRENT_BLOCKCHAIN[-1]
    #         displayBlock(CURRENT_BLOCKCHAIN[-1])
    #         print()
    #         print(f"Consensus Round {len(CURRENT_BLOCKCHAIN)} took {end - start} time")
    #         print(C_TIME)
    #
    #         with minerSelectedCondition:
    #             minerSelected = -1
    #             minerSelectedCondition.notify_all()
    #
    #         with arrayNotEmptyCondition:
    #             miners.append(Miner(self.id, self.name))
    #             arrayNotEmptyCondition.notify_all()
    #             print('Starting new consensus round---------------------')


# Class of Transaction
class Transaction:
    def __init__(self, FROM, TO, AMOUNT, FEE):
        self.FROM = FROM
        self.TO = TO
        self.AMOUNT = AMOUNT
        self.FEE = FEE


# Class of Block
class Block:
    def __init__(self, id, timestamp, current_hash, previous_hash, miner_name, miner_id):
        self.id = id
        self.timestamp = timestamp
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


def select_miner():
    global  CURRENT_BLOCK_ID
    sorted_cand = sorted(candidates, key=lambda cand: cand.id)
    print(len(sorted_cand), sorted_cand)

    r = 0
    d = 1

    while r == 0:
        for x in sorted_cand:
            r = x.id ** d

        r += CURRENT_BLOCK_ID
        r = r % (len(sorted_cand) + 1)
        d += 1

    miner = sorted_cand[r - 1]
    print("Selected miner is ", {miner.name})

    if len(CURRENT_BLOCKCHAIN) == 0:
        prev_hash = "0" * 128
    else:
        prev_hash = CURRENT_BLOCKCHAIN[-1].current_hash

    timestamp_used = time.time()

    newblock = Block(
        CURRENT_BLOCK_ID,
        timestamp_used,
        CHANCE,
        prev_hash,
        miner.name,
        miner.id
    )

    CURRENT_BLOCK_ID += 1
    CURRENT_BLOCKCHAIN.append(newblock)
    MINE_LOG[miner.id]["count"] += 1
    MINE_LOG[miner.id]["block"] = CURRENT_BLOCKCHAIN[-1]
    displayBlock(CURRENT_BLOCKCHAIN[-1])


def run_consensus():
    global TARGET, miners, candidates

    start1 = time.time()

    while time.time() - start1 < p1:
        for x in miners:
            if x in candidates:
                continue

            chash = computeHash(f'{CHANCE}{x.id}')
            if int(chash, 16) / (10 ** 150) < TARGET:
                candidates.append(x)

        if len(candidates) > 15:
            print("Consenus failed because candidates are more than 15")
            break
        elif 15 <= len(candidates) <= 6:
            select_miner()
            break

        if len(candidates) > 1:
            TARGET = TARGET / math.log10(len(candidates))
        else:
            TARGET = TARGET / math.log10(2)

    print(len(candidates), candidates)

    if len(candidates) < 6:
        print("after t1 time candidates are less than 6 so waiting for t2 time")
        start2 = time.time()

        while time.time() - start2 < p2:
            for x in miners:
                if x in candidates:
                    continue

                chash = computeHash(f'{CHANCE}{x.id}')
                if int(chash, 16) / (10 ** 150) < TARGET:
                    candidates.append(x)

            if len(candidates) > 15:
                print("Consenus failed because candidates are more than 15")
                break
            elif 15 <= len(candidates) <= 6:
                select_miner()
                break

            if len(candidates) > 1:
                TARGET = TARGET / math.log10(len(candidates))
            else:
                TARGET = TARGET / math.log10(2)

            print(f"New Target is {TARGET}")

        if len(candidates) < 6:
            print("Consenus failed because candidates are less than 15")



# Main
try:
    # Initial Preparation
    prepareToRun()

    # Global Variable Initialisation
    MINER_COUNT = 20
    CHANCE = 1010
    TARGET = 515
    CURRENT_BLOCK_ID = 1
    CURRENT_BLOCKCHAIN = []
    C_TIME = []
    p1 = 120
    p2 = 60

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

    # Initialising miners
    miners = []
    for i in range(MINER_COUNT):
        miners.append(Miner(id=i + 1, name=f'Miner-{i + 1}'))

    while True:
        c_start = time.time()
        candidates = []
        run_consensus()
        C_TIME.append(time.time() - c_start)
        print(f"Consensus Round {len(CURRENT_BLOCKCHAIN)} took {time.time() - c_start}")

        if len(CURRENT_BLOCKCHAIN) == 0:
            previous_hash = "0" * 128
        else:
            previous_hash = CURRENT_BLOCKCHAIN[-1].current_hash

        CHANCE = computeHash(f'{len(CURRENT_BLOCKCHAIN)}{previous_hash}{len(candidates)}')
        TARGET = TARGET / math.log10(len(candidates))
        CURRENT_BLOCK_ID += 1

        print(f"New values for next round  is chance:{CHANCE},Target: {TARGET},BN:{CURRENT_BLOCK_ID}")

except Exception as e:
    print(e)

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
