from multiprocessing import JoinableQueue, Process, current_process, cpu_count, Manager
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


def all_divisors_of_the_number_1(number: int) -> list:

    if number == 0:
        return []

    subnumber_group = [1]
    for i in range(2, int(pow(number, 0.5)) + 1):  # (number//2)+1
        if number % i == 0:
            subnumber_group.append(i)
            subnumber_group.append(int(number / i))

    subnumber_group.append(number)
    subnumber_group.sort()

    return subnumber_group


@fix_duration
def factorize(*numbers):
    logger.debug("START factorize")
    rez = []
    for number in numbers:
        subnumber_group = all_divisors_of_the_number_1(number)
        rez.append(subnumber_group)

    logger.debug("END factorize")

    return rez


@fix_duration
def factorize_1(*numbers):
    logger.debug("START factorize_1")
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

    logger.debug("END factorize_1")

    return rez


def all_divisors_of_the_number(
    jqueue: JoinableQueue, return_dict: Manager().dict()
) -> None:
    name = current_process().name
    logger.debug(f"{name} started...")
    number = jqueue.get()

    if number == 0:
        jqueue.task_done()
        # return []
        return_dict[number] = []

    subnumber_group = [1]
    for i in range(2, int(pow(number, 0.5)) + 1):  # (number//2)+1
        if number % i == 0:
            subnumber_group.append(i)
            subnumber_group.append(int(number / i))

    subnumber_group.append(number)
    subnumber_group.sort()
    # jqueue.task_done()

    # return subnumber_group
    return_dict[number] = subnumber_group
    jqueue.task_done()
    sys.exit(0)


@fix_duration
def factorize_process(*numbers):
    logger.debug("START factorize_process")
    manager = Manager()
    return_dict = manager.dict()

    joinable_queue = JoinableQueue()

    # for number in numbers:  # range(2) or max_relevant_threads
    #     Process(
    #         target=all_divisors_of_the_number, args=(joinable_queue, return_dict)
    #     ).start()
    #     joinable_queue.put(number)

    [
        joinable_queue.put(number)
        for number in numbers
        if not Process(
            target=all_divisors_of_the_number, args=(joinable_queue, return_dict)
        ).start()
    ]
    # for number in numbers:
    #     joinable_queue.put(number)
    # rez.append(all_divisors_of_the_number(number))

    joinable_queue.join()
    logger.debug("END factorize_process")

    return return_dict.values()


if __name__ == "__main__":

    print(f"{max_relevant_threads=}")

    print(factorize_1(128, 255, 99999, 10651060))

    print(factorize(128, 255, 99999, 10651060))

    print(factorize_process(128, 255, 99999, 10651060))
    #
    # a, b, c, d = factorize(128, 255, 99999, 10651060)
    #
    # assert a == [1, 2, 4, 8, 16, 32, 64, 128]
    # assert b == [1, 3, 5, 15, 17, 51, 85, 255]
    # assert c == [1, 3, 9, 41, 123, 271, 369, 813, 2439, 11111, 33333, 99999]
    # assert d == [
    #     1,
    #     2,
    #     4,
    #     5,
    #     7,
    #     10,
    #     14,
    #     20,
    #     28,
    #     35,
    #     70,
    #     140,
    #     76079,
    #     152158,
    #     304316,
    #     380395,
    #     532553,
    #     760790,
    #     1065106,
    #     1521580,
    #     2130212,
    #     2662765,
    #     5325530,
    #     10651060,
    # ]
    #
    # a, b, c, d = factorize_process(128, 255, 99999, 10651060)
    #
    # assert a == [1, 2, 4, 8, 16, 32, 64, 128]
    # assert b == [1, 3, 5, 15, 17, 51, 85, 255]
    # assert c == [1, 3, 9, 41, 123, 271, 369, 813, 2439, 11111, 33333, 99999]
    # assert d == [
    #     1,
    #     2,
    #     4,
    #     5,
    #     7,
    #     10,
    #     14,
    #     20,
    #     28,
    #     35,
    #     70,
    #     140,
    #     76079,
    #     152158,
    #     304316,
    #     380395,
    #     532553,
    #     760790,
    #     1065106,
    #     1521580,
    #     2130212,
    #     2662765,
    #     5325530,
    #     10651060,
    # ]
