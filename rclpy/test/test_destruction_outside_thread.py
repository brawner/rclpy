# Copyright 2019 PickNik Consulting
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ProcessPoolExecutor has the capability of gracefully handling segfaults in its ProcessPool.
# By graceful, I mean the entire pool goes up in flames, but future.result() will just throw an
# BrokenPoolException, so that's suitable for these tests.
from concurrent.futures import ProcessPoolExecutor, wait

import faulthandler

import sys
from threading import Thread
import time
import traceback

import rclpy
from rclpy.duration import Duration
from std_msgs.msg import String

# Dumps the stack trace to stderr when a fault occurs
faulthandler.enable()


class RclpySpinner(Thread):

    def __init__(self, node, name='', spin_once_timeout=1.0):
        super().__init__()
        self.name = name
        self.node = node
        self.exited_normally = False
        self.abort = False
        self.spin_once_timeout = spin_once_timeout

    def run(self):
        print('Starting Spinner: {}'.format(self.name))
        while rclpy.ok() and not self.abort:
             rclpy.spin_once(self.node, timeout_sec=self.spin_once_timeout)

        self.exited_normally = True

    def shutdown(self):
        print('Shutdown called on RclpySpinner: {}'.format(self.name))
        self.abort = True


def run_catch_report_raise(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception:
        print('exception in {0}():'.format(func.__name__), file=sys.stderr)
        traceback.print_exc()
        raise


def func_publisher_chatter(timer_period=0.001, publisher_alive_time=3.0):
    rclpy.init()
    node = rclpy.create_node('publisher')
    pub = node.create_publisher(String, '/chatter')

    def publisher_callback():
        pub.publish(String(data='hello'))

    tmr = node.create_timer(timer_period, publisher_callback)
    spinner = RclpySpinner(node, 'publisher')
    spinner.start()

    # Publisher needs to stay alive long enough for subscriber process
    print('Publisher sleeping for {}'.format(publisher_alive_time))
    time.sleep(publisher_alive_time)

    print('Killing spinner')
    spinner.shutdown()
    print("Destroying timer")
    node.destroy_timer(tmr)
    print("Destroying node")
    node.destroy_node()
    print("Shutting down")
    rclpy.shutdown()
    time.sleep(spinner.spin_once_timeout)
    return True


def func_subscriber_chatter(
        subscription_alive_time=0.1, num_subscribers=100):
    print('Initializing subscriber')
    rclpy.init()
    node = rclpy.create_node('subscriber')
    spinner = RclpySpinner(node, 'subscriber')
    spinner.start()
    print('Creating subscriptions')
    for i in range(num_subscribers):
        sub = node.create_subscription(String, '/chatter', lambda msg: 1 + 1)
        # Allow the subscriber some time to do its thing
        time.sleep(subscription_alive_time)
        print('Destroying sub {}'.format(i))
        node.destroy_subscription(sub)

    # Wait for other thread to exit normally
    print('Shutting down spinner')
    spinner.shutdown()
    print('Shutting down subscriber node')
    node.destroy_node()
    rclpy.shutdown()
    time.sleep(spinner.spin_once_timeout)
    return spinner.exited_normally


def func_destroy_node_while_spin_once(spin_once_timeout=0.1, spinner_wait=0.1):
    print('Initializing node')
    rclpy.init()
    node = rclpy.create_node('n')
    spinner = RclpySpinner(node, spin_once_timeout=spin_once_timeout)
    spinner.start()

    # Needs sometime to get to _wait_for_ready_callbacks
    print('Waiting for node to spin')
    time.sleep(spinner_wait)

    print('Shutting down')
    node.destroy_node()
    rclpy.shutdown()
    time.sleep(spinner.spin_once_timeout)
    return spinner.exited_normally


def func_publisher_spin_once(timer_period=0.001, publisher_alive_time=1.0, num_publishers=100):
    rclpy.init()
    node = rclpy.create_node('publisher')
    spinner = RclpySpinner(node, 'publisher')
    spinner.start()
    for i in range(num_publishers):
        pub = node.create_publisher(String, '/chatter')
        tmr = node.create_timer(timer_period, lambda: pub.publish(String(data='hello')))
        time.sleep(publisher_alive_time)

        print('Destroying timer {}'.format(i))
        node.destroy_timer(tmr)

    print("Destroying node")
    node.destroy_node()
    print("Shutting down")
    rclpy.shutdown()
    time.sleep(spinner.spin_once_timeout)
    return True


def test_destroy_subscriptions_out_of_thread():
    """
    Test destroying subscription out of thread.

    Results in segfault in subscriber process 100%.
    """
    publisher_timer_period = 0.001
    publisher_alive_time = 10.0
    subscription_alive_time = 0.1
    num_subscribers = 100

    with ProcessPoolExecutor() as executor:
        print("Starting publisher")
        publisher_future = executor.submit(
                func_publisher_chatter, publisher_timer_period, publisher_alive_time)
        print("Starting subscriber")
        subscriber_future = executor.submit(
            func_subscriber_chatter, subscription_alive_time, num_subscribers)

        # Subscription future is the code under test, only wait for it (not publisher)
        print("Waiting subscriber")
        wait([subscriber_future])

        # raises BrokenProcessPool
        print("Getting Result")
        result = subscriber_future.result()

        print("Result {}".format(result))
        assert result


def test_destroy_node_while_spin_once():
    """
    Test destroying node while it is spinning in other thread.

    Results in segfault 100% at the same place
    """
    spin_once_timeout = 0.1
    spinner_wait = 0.1

    with ProcessPoolExecutor() as executor:
        future = \
            executor.submit(func_destroy_node_while_spin_once, spin_once_timeout, spinner_wait)

        # raises BrokenProcessPool
        print("Getting Result")
        result = future.result()

        print("Result {}".format(result))
        assert result


def test_destroy_publisher_while_spin_once():
    """
    Test destroying timer while node is spinning in other thread.

    This test fails 100%. 75% after destroying timer, 25% after destroying the node or shutting down
    """
    timer_period = 0.001
    publisher_alive_time = 0.1
    num_publishers = 100
    with ProcessPoolExecutor() as executor:
        future = executor.submit(
            func_publisher_spin_once, timer_period, publisher_alive_time, num_publishers)

        # raises BrokenProcessPool
        print("Getting Result")
        result = future.result()

        print("Result {}".format(result))
        assert result


if __name__ == "__main__":
    test_destroy_node_while_spin_once()
    test_destroy_publisher_while_spin_once()
    test_destroy_subscriptions_out_of_thread()
