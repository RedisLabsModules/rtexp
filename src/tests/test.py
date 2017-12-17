#! /usr/bin/python
import redis
import time
import random
import sys

MS_COMPARISON_ACCURACY = 2

def compare_ms(a, b):
    return (abs(a-b) <= MS_COMPARISON_ACCURACY)

def current_time_ms():
    millis = int(round(time.time() * 1000))
    return millis

# 1. REXPIRE {key} {ttl_ms}
# 3. RTTL {key}
def test_REXPIRE_RTTL(redis_service):
    retval = False
    ttl_ms = 10000
    start_ts = current_time_ms()
    div = random.randint(0,ttl_ms/2)

    key = "set_get_test_key"
    redis_service.execute_command("SET", key, 1)
    redis_service.execute_command("REXPIRE", key, ttl_ms)
    time.sleep(div/1000.0)
    sleep_time = current_time_ms() - start_ts
    expected = ttl_ms - sleep_time
    saved_ms = redis_service.execute_command("RTTL", key)
    if (not compare_ms(saved_ms,expected)):
        sys.stdout.write("ERROR: expected {} but found {}\n".format(expected, saved_ms))
        retval = False
    else:
        retval = True

    return retval


# 2. REXPIREAT {key} {timestamp_ms}
# 3. RTTL {key}
def test_REXPIREAT_RTTL(redis_service):
    retval = False
    ttl_ms = 10000
    start_ts = current_time_ms()
    timestamp_ms = current_time_ms() + ttl_ms
    div = random.randint(0,ttl_ms/2)
    key = "set_at_test_key"
    redis_service.execute_command("SET", key, 1)
    redis_service.execute_command("REXPIREAT", key, timestamp_ms)
    time.sleep(div/1000.0)
    sleep_time = current_time_ms() - start_ts
    expected = ttl_ms - sleep_time
    saved_ms = redis_service.execute_command("RTTL", key)
    if (not compare_ms(saved_ms,expected)):
        sys.stdout.write("ERROR: expected {} but found {}\n".format(expected, saved_ms))
        retval = False
    else:
        retval = True

    return retval


# 4. RUNEXPIRE {key}
def test_RUNEXPIRE(redis_service):
    retval = False
    ttl_ms = 10000
    div = random.randint(0,ttl_ms/4)
    expected = -2
    key = "del_exp_test_key"
    redis_service.execute_command("SET", key, 1)
    redis_service.execute_command("REXPIRE", key, ttl_ms)
    redis_service.execute_command("RUNEXPIRE", key)
    time.sleep(div/1000.0)
    saved_ms = redis_service.execute_command("RTTL", key)
    if (not compare_ms(saved_ms,expected)):
        sys.stdout.write("ERROR: expected {} but found {}\n".format(expected, saved_ms))
        rcompare_msetval = False
    else:
        retval = True

    return retval


# 5. RSETEX {key} {value} {ttl}
def test_RSETEX(redis_service):
    retval = False
    start_ts = current_time_ms()
    ttl_ms = 10000
    value = "test_value"
    expected_ms = ttl_ms
    key = "set_get_test_key"
    redis_service.execute_command("SET", key, 1)
    redis_service.execute_command("RSETEX", key, value, ttl_ms)
    saved_ms = redis_service.execute_command("RTTL", key)
    saved_value = redis_service.execute_command("GET", key)
    if (not compare_ms(saved_ms, expected_ms)):
        sys.stdout.write("ERROR: expected {} but found {}\n".format(expected_ms, saved_ms))
        retval = False
    elif not saved_value:
        sys.stdout.write("ERROR: expected {} but was not found\n".format(value))
        retval = False
    elif (saved_value != value):
        sys.stdout.write("ERROR: expected {} but found {}\n".format(value, saved_value))
        retval = False
    else:
        retval = True

    return retval


# 5. REXECEX {cmd} {key} {ttl_ms} {....}
def test_REXECEX(redis_service):
    retval = False
    start_ts = current_time_ms()
    ttl_ms = 10000
    value = "test_value"
    expected_ms = ttl_ms
    key = "set_get_test_key"
    redis_service.execute_command("SET", key, 1)
    redis_service.execute_command("REXECEX", "SET", key, ttl_ms, value)
    saved_ms = redis_service.execute_command("RTTL", key)
    saved_value = redis_service.execute_command("GET", key)
    if (not compare_ms(saved_ms, expected_ms)):
        sys.stdout.write("ERROR: expected {} but found {}\n".format(expected_ms, saved_ms))
        retval = False
    elif not saved_value:
        sys.stdout.write("ERROR: expected {} but was not found\n".format(value))
        retval = False
    elif (saved_value != value):
        sys.stdout.write("ERROR: expected {} but found {}\n".format(value, saved_value))
        retval = False
    else:
        retval = True

    return retval



def run_internal_test(redis_service):
    sys.stdout.write("module functional test (internal) - \n")
    sys.stdout.flush()
    sys.stdout.write("UNIMPLEMENTED!\n")
    return True
    sys.stdout.write(redis_service.execute_command("RTEXP.TEST"))


def function_test_rtexp(redis_service):
    sys.stdout.write("module functional test (external) - \n")
    sys.stdout.flush()
    start_time = current_time_ms()
    num_of_FAILED_tests = 0
    num_of_passed_tests = 0

    sys.stdout.write("testing REXPIRE_RTTL: ")
    if (test_REXPIRE_RTTL(redis_service) == False):
        num_of_FAILED_tests += 1
        sys.stdout.write("FAILED\n")
    else:
        sys.stdout.write("PASSED\n")
        num_of_passed_tests += 1

    sys.stdout.write("\ntesting REXPIREAT_RTTL: ")
    if (test_REXPIREAT_RTTL(redis_service) == False):
        num_of_FAILED_tests +=1
        sys.stdout.write("FAILED\n")
    else:
        sys.stdout.write("PASSED\n")
        num_of_passed_tests +=1

    sys.stdout.write("\ntesting RUNEXPIRE: ")
    if (test_RUNEXPIRE(redis_service) == False):
        num_of_FAILED_tests +=1
        sys.stdout.write("FAILED\n")
    else:
        sys.stdout.write("PASSED\n")
        num_of_passed_tests +=1

    sys.stdout.write("\ntesting RSETEX: ")
    if (test_RSETEX(redis_service) == False):
        num_of_FAILED_tests +=1
        sys.stdout.write("FAILED\n")
    else:
        sys.stdout.write("PASSED\n")
        num_of_passed_tests +=1

    sys.stdout.write("\ntesting REXECEX: ")
    if (test_REXECEX(redis_service) == False):
        num_of_FAILED_tests +=1
        sys.stdout.write("FAILED\n")
    else:
        sys.stdout.write("PASSED\n")
        num_of_passed_tests +=1

    total_time_ms = current_time_ms() - start_time
    sys.stdout.write("-------------\n")
    if (num_of_FAILED_tests):
        sys.stdout.write("FAILED ({} tests FAILED and {} passed in {} sec)\n".format(num_of_FAILED_tests,num_of_passed_tests, total_time_ms / 1000))
        return False
    else:
        sys.stdout.write("OK ({} tests passed in {} sec)\n".format(num_of_passed_tests, total_time_ms / 1000))
        return True

def load_test_rtexp(redis_service, timers=1000000, timeouts=[1, 10000]):#, 2, 4, 16, 32, 100, 200, 1000, 2000, 4000, 10000]):
    print "starting load tests"
    start = time.time()
    for i in range(timers):
        if (i%100000 == 0):
            print "set {} timers".format(i)
        ttl_ms = random.choice(timeouts)
        key = "timer_{}_for_{}_ms".format(i, ttl_ms)
        redis_service.execute_command("SET", key, 1)
        redis_service.execute_command("REXPIRE", key, ttl_ms)

    timer_count = redis_service.execute_command("RCOUNT")
    while (timer_count):
        print "Done setting timers, waiting for remaining {} to expire".format(redis_service.execute_command("RCOUNT"))
        time.sleep(5)
        timer_count = redis_service.execute_command("RCOUNT")

    end = time.time()
    print "All timers expired, printing results"
    redis_service.execute_command("RPROFILE")
    print "mesured {} timers in {} sec".format(timers, end-start)
    return True

    # print "mean push velocity =", cycles/(push_end-start), "per second"
    # print "mean push(generating ids) velocity =", cycles/(gid_push_end-push_end), "per second"
    # print "mean pull velocity =", cycles/(pull_end-gid_push_end), "per second"
    # print "mean poll velocity = ", cycles/poll_sum, "per second"

if __name__ == "__main__":
    args = sys.argv[1:]
    test_internal = False
    test_external = False
    load_test = False
    port = 6379
    if not args:
        test_internal = False # TODO: True?
        test_external = True
        load_test = True
    else:
        for i, arg in enumerate(args):
            if arg == "--port":
                port = args[i+1]
            if arg == "--load":
                load_test = True
            if arg == "--noload":
                load_test = False
                test_internal = True
                test_external = True
            elif arg == "--internal":
                test_internal = True
            elif arg == "--external":
                test_external = True

    print "port", port
    r = redis.StrictRedis(host='localhost', port=port, db=0)
    if test_internal:
        run_internal_test(r)
    if test_external:
        function_test_rtexp(r)
    if load_test:
        load_test_rtexp(r)
