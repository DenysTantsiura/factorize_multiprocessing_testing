from multiprocessing import JoinableQueue, Process, current_process, cpu_count
import logging
import sys
from time import time

max_relevant_threads = cpu_count()

logger = logging.getLogger("Logging factorize")
logger.setLevel(logging.DEBUG)
command_line_hendler = logging.StreamHandler()
command_line_hendler.setLevel(logging.DEBUG)
log_formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
command_line_hendler.setFormatter(log_formatter)
logger.addHandler(command_line_hendler)


def fix_duration(my_function):
    def inner(*args):
        start = time()
        rezult = my_function(*args)
        print(f"Duration({my_function.__name__}): {time() - start} sec")

        return rezult

    return inner


def factorize_1(*numbers):
    logger.debug("START factorize_1")
    rez = []
    for number in numbers:
        subnumber_group = [1]
        for i in range(2, (number // 2) + 1):
            if number % i == 0:
                subnumber_group.append(i)

        subnumber_group.append(number)
        rez.append(subnumber_group)

    logger.debug("END factorize_1")

    return rez


def factorize_2(*numbers):
    logger.debug("START factorize_2")
    rez = []
    for number in numbers:
        subnumber_group = [1]
        for i in range(2, int(pow(number, 0.5)) + 1):  # (number//2)+1
            if number % i == 0:
                subnumber_group.append(i)
                subnumber_group.append(int(number / i))

        subnumber_group.append(number)
        subnumber_group.sort()
        rez.append(subnumber_group)

    logger.debug("END factorize_2")

    return rez


def double_variant(my_function):
    def inner(*args):
        rezult_1 = my_function(*args)

        start = time()
        rezult_2 = factorize_2(*args)
        print(f"Duration(factorize_2): {time() - start} sec")

        if rezult_1 == rezult_2:
            logger.debug("All calculations are equal.")
            return rezult_1

        logger.warning("Something is calculated WRONG!")
        return rezult_1, rezult_2

    return inner


@double_variant
@fix_duration
def factorize(*numbers):
    return factorize_1(*numbers)


# joinable_queue = JoinableQueue()


def all_divisors_of_the_number(jqueue: JoinableQueue, rez: list) -> None:
    name = current_process().name
    logger.debug(f"{name} started...")
    number = jqueue.get()

    if number == 0:
        jqueue.task_done()
        # return []
        rez.append([])

    subnumber_group = [1]
    for i in range(2, int(pow(number, 0.5)) + 1):  # (number//2)+1
        if number % i == 0:
            subnumber_group.append(i)
            subnumber_group.append(int(number / i))

    subnumber_group.append(number)
    subnumber_group.sort()
    # jqueue.task_done()

    # return subnumber_group
    rez.append(subnumber_group)
    jqueue.task_done()
    sys.exit(0)


def factorize_process(*numbers):
    rez = []
    calculation_process = {}
    joinable_queue = JoinableQueue()

    for number in numbers:
        joinable_queue.put(number)
        # rez.append(all_divisors_of_the_number(number))

    for cpu_flow in range(2):  # 2 or max_relevant_threads
        calculation_process[cpu_flow] = Process(
            target=all_divisors_of_the_number, args=(joinable_queue, rez)
        )
        calculation_process[cpu_flow].start()

    joinable_queue.join()

    return rez


print(factorize(128, 255, 99999, 10651060))

print(max_relevant_threads)

print(factorize_process(128, 255, 99999, 10651060))
# max_relevant_threads
