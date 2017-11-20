#! /usr/bin/python
import redis
import time
import random
import sys



def current_time_ms():
    millis = int(round(time.time() * 1000))
    return millis

# 1. REXPIRE {key} {ttl_ms}
# 3. RTTL {key}
def test_REXPIRE_RTTL(redis_service):
    retval = False
    ttl_ms = 10000
    expected = current_time_ms() + ttl_ms
    key = "set_get_test_key"
    redis_service.execute_command("rtexp.REXPIRE", key, ttl_ms)
    saved_ms = redis_service.execute_command("rtexp.RTTL", key)
    if (saved_ms != expected):
        sys.stdout.write("ERROR: expected {} but found {}\n".format(expected, saved_ms))
        retval = False
    else:
        retval = True

    redis_service.execute_command("rtexp.DEL", store)
    return retval


# 2. REXPIREAT {key} {timestamp_ms}
# 3. RTTL {key}
def test_REXPIREAT_RTTL(redis_service):
    retval = False
    timestamp_ms = current_time_ms() + 10000
    expected = timestamp_ms
    key = "set_at_test_key"
    redis_service.execute_command("rtexp.REXPIREAT", key, timestamp_ms)
    saved_ms = redis_service.execute_command("rtexp.RTTL", key)
    if (saved_ms != expected):
        sys.stdout.write("ERROR: expected {} but found {}\n".format(expected, saved_ms))
        retval = False
    else:
        retval = True

    redis_service.execute_command("rtexp.DEL", store)
    return retval


# 4. RUNEXPIRE {key}
def test_RUNEXPIRE(redis_service):
    retval = False
    ttl_ms = 10000
    expected = -1
    key = "del_exp_test_key"
    redis_service.execute_command("rtexp.REXPIRE", key, ttl_ms)
    redis_service.execute_command("rtexp.RUNEXPIRE", key)
    saved_ms = redis_service.execute_command("rtexp.RTTL", key)
    if (saved_ms != expected):
        sys.stdout.write("ERROR: expected {} but found {}\n".format(expected, saved_ms))
        retval = False
    else:
        retval = True

    redis_service.execute_command("rtexp.DEL", store)
    return retval


# 5. RSETEX {key} {value} {ttl}
def test_RSETEX(redis_service):
    retval = False
    ttl_ms = 10000
    value = "test_value"
    expected_ms = current_time_ms() + ttl_ms
    key = "set_get_test_key"
    redis_service.execute_command("rtexp.RSETEX", key, value, ttl_ms)
    saved_ms = redis_service.execute_command("rtexp.RTTL", key)
    saved_value = redis_service.execute_command("rtexp.RSETEX", key, value, ttl_ms)
    if (saved_ms != expected_ms):
        sys.stdout.write("ERROR: expected {} but found {}\n".format(expected_ms, saved_ms))
        retval = False
    elif not saved_value:
        sys.stdout.write("ERROR: expected {} but was not found\n".format(value))
        retval = False
    elif (saved_value == value):
        sys.stdout.write("ERROR: expected {} but found {}\n".format(value, saved_value))
        retval = False
    else:
        retval = True


def run_internal_test(redis_service):
    sys.stdout.write("module functional test (internal) - ")
    sys.stdout.flush()
    sys.stdout.write("UNIMPLEMENTED!")
    return True
    sys.stdout.write(redis_service.execute_command("RTEXP.TEST"))


def function_test_rtexp(redis_service):
    sys.stdout.write("module functional test (external) - ")
    sys.stdout.flush()
    start_time = current_time_ms()
    num_of_FAILED_tests = 0
    num_of_passed_tests = 0

    if (test_REXPIRE_RTTL(redis_service) == False):
        num_of_FAILED_tests += 1
        sys.stdout.write("FAILED on REXPIRE_RTTL")
    else:
        sys.stdout.write("PASSED REXPIRE_RTTL test")
        num_of_passed_tests += 1

    if (test_REXPIREAT_RTTL(redis_service) == False):
        num_of_FAILED_tests +=1
        sys.stdout.write("FAILED on REXPIREAT_RTTL")
    else:
        sys.stdout.write("PASSED REXPIREAT_RTTL test")
        num_of_passed_tests +=1
  

    if (test_RUNEXPIRE(redis_service) == False):
        num_of_FAILED_tests +=1
        sys.stdout.write("FAILED on RUNEXPIRE")
    else:
        sys.stdout.write("PASSED RUNEXPIRE test")
        num_of_passed_tests +=1


    if (test_RSETEX(redis_service) == False):
        num_of_FAILED_tests +=1
        sys.stdout.write("FAILED on RSETEX")
    else:
        sys.stdout.write("PASSED RSETEX test")
        num_of_passed_tests +=1

    total_time_ms = current_time_ms() - start_time
    sys.stdout.write("-------------")
    if (num_of_FAILED_tests):
        sys.stdout.write("FAILED ({} tests FAILED and {}} passed in {}} sec)".format(num_of_FAILED_tests,num_of_passed_tests, total_time_ms / 1000))
        return False
    else:
        sys.stdout.write("OK ({} tests passed in {} sec)".format(num_of_passed_tests, total_time_ms / 1000))
        return True

def load_test_rtexp(redis_service, cycles=1000000, timeouts=[1,2,4,16,32,100,200,1000]):
    redis_service.execute_command("DEL", "python_load_test_rtexp")
    print "starting load tests"
    print "UNIMPLEMENTED!"
    return True

    print "measuring PUSH"
    start = time.time()
    # test push
    for i in range(cycles):
        redis_service.execute_command("rtexp.push", "python_load_test_rtexp", random.choice(timeouts)*1000, "payload", "%d" % i)
    push_end = time.time()


    print "measuring GIDPUSH (auto generating ids)"
    # test push
    for i in range(cycles):
        redis_service.execute_command("rtexp.gidpush", "python_load_test_rtexp", random.choice(timeouts)*1000, "payload")
    gid_push_end = time.time()


    print "measuring PULL"
    for i in range(cycles):
        redis_service.execute_command("rtexp.pull", "python_load_test_rtexp", "%d" % i)
    pull_end = time.time()

    print "preparing POLL"
    start_i = 0
    end_i = cycles/3
    for j in range(3):
        for i in range(start_i,end_i):
            redis_service.execute_command("rtexp.push", "python_load_test_rtexp", "%d" % i, "payload", 1000*(3-j+random.choice([1,2,3])))
        start_i += cycles/3
        end_i += cycles/3
        time.sleep(1)

    print "measuring POLL"
    poll_sum = 0
    for j in range(10):
        time.sleep(1)
        poll_start = time.time()
        redis_service.execute_command("rtexp.poll", "python_load_test_rtexp")
        poll_end = time.time()
        poll_sum += poll_end-poll_start

    print "mean push velocity =", cycles/(push_end-start), "per second"
    print "mean push(generating ids) velocity =", cycles/(gid_push_end-push_end), "per second"
    print "mean pull velocity =", cycles/(pull_end-gid_push_end), "per second"
    print "mean poll velocity = ", cycles/poll_sum, "per second"
    redis_service.execute_command("DEL", "python_load_test_rtexp")

if __name__ == "__main__":
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    args = sys.argv[1:]
    test_internal = False
    test_external = False
    load_test = False
    if not args:
        test_internal = False # TODO: True
        test_external = True
        load_test = False # TODO: True
    else:
        for arg in args:
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

    if test_internal:
        run_internal_test(r)
    if test_external:
        function_test_rtexp(r)
    if load_test:
        load_test_rtexp(r)
