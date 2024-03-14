################### 
# Course: CSE138
# Date: Winter 2024
# Assignment: 4
# This document is the copyrighted intellectual property of the authors.
# Do not copy or distribute in any form without explicit permission.
###################

import collections
import subprocess
import unittest
import requests
import random
import time
import os


### initialize constants

hostname = 'localhost' # Windows and Mac users can change this to the docker vm ip
hostBaseUrl = 'http://{}'.format(hostname)

imageName = "asg4img"
subnetName = "asg4net"
subnetRange = "10.10.0.0/16"
containerPort = "8090"

class InstanceConfig(collections.namedtuple('InstanceConfig', ['name', 'addr', 'published_port'])):
    @property
    def socket_address(self):
        return '{}:{}'.format(self.addr, containerPort)
    def __str__(self):
        return self.name

alice = InstanceConfig(name='alice', addr='10.10.0.2', published_port=8082)
bob   = InstanceConfig(name='bob',   addr='10.10.0.3', published_port=8083)
carol = InstanceConfig(name='carol', addr='10.10.0.4', published_port=8084)
dave  = InstanceConfig(name='dave',  addr='10.10.0.5', published_port=8085)
erin  = InstanceConfig(name='erin',  addr='10.10.0.6', published_port=8086)
frank = InstanceConfig(name='frank', addr='10.10.0.7', published_port=8087)
grace = InstanceConfig(name='grace', addr='10.10.0.8', published_port=8088)
# "grace" is an extra instance not included in "all_instances" or in the initial view
all_instances = [alice, bob, carol, dave, erin, frank]
viewStr = lambda instances: ','.join(r.socket_address for r in instances)
viewSet = lambda instances: set(r.socket_address for r in instances)

def sleep(n):
    multiplier = 1
    # Increase the multiplier if you need to during debugging, but make sure to
    # set it back to 1 and test your work before submitting.
    print('(sleeping {} seconds)'.format(n*multiplier))
    time.sleep(n*multiplier)


### docker linux commands

def removeSubnet(required=True):
    command = ['docker', 'network', 'rm', subnetName]
    print('removeSubnet:', ' '.join(command))
    subprocess.run(command, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, check=required)

def createSubnet():
    command = ['docker', 'network', 'create',
            '--subnet={}'.format(subnetRange), subnetName]
    print('createSubnet:', ' '.join(command))
    subprocess.check_call(command, stdout=subprocess.DEVNULL)

def buildDockerImage():
    command = ['docker', 'build', '-t', imageName, '.']
    print('buildDockerImage:', ' '.join(command))
    subprocess.check_call(command)

def runInstance(instance, view_instances, shard_count=None):
    assert view_instances, 'the view can\'t be empty because it must at least contain this Instance'
    command = ['docker', 'run', '--rm', '--detach',
        '--publish={}:{}'.format(instance.published_port, containerPort),
        "--net={}".format(subnetName),
        "--ip={}".format(instance.addr),
        "--name={}".format(instance.name),
        "-e=SOCKET_ADDRESS={}:{}".format(instance.addr, containerPort),
        "-e=VIEW={}".format(viewStr(view_instances)),
        imageName]
    if shard_count is not None:
        command.insert(-1, "-e=SHARD_COUNT={}".format(shard_count))
    print('runInstance:', ' '.join(command))
    subprocess.check_call(command)

def stopAndRemoveInstance(instance, required=True):
    command = ['docker', 'stop', instance.name]
    print('stopAndRemoveInstance:', ' '.join(command))
    subprocess.run(command, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, check=required)
    command = ['docker', 'remove', instance.name]
    print('stopAndRemoveInstance:', ' '.join(command))
    subprocess.run(command, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, check=required)

def killInstance(instance, required=True):
    '''Kill is sufficient when containers are run with `--rm`'''
    command = ['docker', 'kill', instance.name]
    print('killInstance:', ' '.join(command))
    subprocess.run(command, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, check=required)

def connectToNetwork(instance):
    command = ['docker', 'network', 'connect', subnetName, instance.name]
    print('connectToNetwork:', ' '.join(command))
    subprocess.check_call(command)

def disconnectFromNetwork(instance):
    command = ['docker', 'network', 'disconnect', subnetName, instance.name]
    print('disconnectFromNetwork:', ' '.join(command))
    subprocess.check_call(command)


### test suite

class TestHW4(unittest.TestCase):

    # constants
    shard_count = 2
    key_count = 50 # 600

    @classmethod
    def setUpClass(cls):
        # class properties used as shared-state (via internal mutability) across the whole test suite
        cls.shard_ids = list() # List of shard ids
        cls.shard_members = dict() # Dictionary from shard ids to member lists
        cls.causal_metadata = dict(metadata=None) # Dictionary from key "metadata" to metadata returned by an instance

        print('= Cleaning up resources possibly left over from a previous run..')
        # we use stop-and-remove here because it's not guaranteed that these instances were started with --rm
        stopAndRemoveInstance(grace, required=False) # grace is the extra instance not part of all_instances
        for instance in all_instances:
            stopAndRemoveInstance(instance, required=False)
        removeSubnet(required=False)
        sleep(1)
        print("= Creating resources required for this run..")
        createSubnet()
        # also start instances here; they'll stay up through all the tests
        for instance in all_instances:
            runInstance(instance, all_instances, shard_count=cls.shard_count)
        sleep(5) # give time for them to bind ports, update views, etc..
        os.system("docker ps")

    # @classmethod
    # def tearDownClass(cls):
    #     print("= Cleaning up resources from this run..")
    #     killInstance(grace, required=False) # grace is the extra instance not part of all_instances
    #     for instance in all_instances:
    #         killInstance(instance)
    #     removeSubnet()


    def test_a_get_shard_ids(self):
        '''Do all the instances return the same shard IDs?'''

        print('>>> Get shard-ids from Alice')
        response = requests.get('http://{}:{}/shard/ids'.format(hostname, alice.published_port))
        self.assertEqual(response.status_code, 200)
        self.assertIn('shard-ids', response.json())
        shard_ids = response.json()['shard-ids']

        print('=== Check that everybody reports those shard IDs')
        for instance in all_instances:
            with self.subTest(msg='at instance {}'.format(instance)):
                response = requests.get('http://{}:{}/shard/ids'.format(hostname, instance.published_port))
                self.assertEqual(response.status_code, 200)
                self.assertIn('shard-ids', response.json())
                self.assertEqual(set(response.json()['shard-ids']), set(shard_ids))

        # store the shard ids for the rest of the test suite
        self.shard_ids.extend(shard_ids)


    def test_b_shard_id_members(self):
        '''Do all the instances agree about the members of each shard?'''

        for shard_id in self.shard_ids:
            with self.subTest(msg='for shard {}'.format(shard_id)):
                print('>>> Get shard {} members from Alice'.format(shard_id))
                response = requests.get('http://{}:{}/shard/members/{}'.format(hostname, alice.published_port, shard_id))
                self.assertEqual(response.status_code, 200)
                self.assertIn('shard-members', response.json())
                shard_members = response.json()['shard-members']
                self.assertGreater(len(shard_members), 1)

            print('=== Check that everybody reports those shard {} members'.format(shard_id))
            for instance in all_instances: 
                with self.subTest(msg='for shard {}; at instance {}'.format(shard_id, instance)):
                    response = requests.get('http://{}:{}/shard/members/{}'.format(hostname, instance.published_port, shard_id))
                    self.assertEqual(response.status_code, 200)
                    self.assertIn('shard-members', response.json())
                    instance_reported = response.json()['shard-members']
                    self.assertEqual(set(instance_reported), set(shard_members))

            # store the shard member socket addresses for the rest of the test suite
            self.shard_members[shard_id] = shard_members

        self.assertEqual(len(all_instances), sum(len(members) for shard_id, members in self.shard_members.items()))


    def test_c_node_shard_id(self):
        '''Does each instance's shard-id exist in the saved shard_ids and shard_members?'''

        for instance in all_instances:
            with self.subTest(msg='at instance {}'.format(instance)):
                print('>>> Get the node-shard-id from {}'.format(instance))
                response = requests.get('http://{}:{}/shard/node-shard-id'.format(hostname, instance.published_port))
                self.assertEqual(response.status_code, 200)
                self.assertIn('node-shard-id', response.json())
                instance_shard_id = response.json()['node-shard-id']

                print('=== Check that the {}\'s id {} fits with previously collected data'.format(instance, instance_shard_id))
                self.assertIn(instance_shard_id, self.shard_ids)
                self.assertIn(instance_shard_id, self.shard_members)
                self.assertIn(instance.socket_address, self.shard_members[instance_shard_id])


    def test_d_put_key_value_operation(self):
        '''Do the replicas keep up when broadcasting with many causally-dependent requests issued quickly?'''

        print('>>> Put {} key:value pairs into the store.'.format(self.key_count))
        for n in range(self.key_count):

            key = 'key{}'.format(n)
            value = 'value{}'.format(n)
            instance = all_instances[n % len(all_instances)]
            print('>>> Put {key}:{value} at instance {instance} (with retries)'.format(key=key, value=value, instance=instance))

            retries = 7
            backoffSec = lambda attempt: 0.01*2**attempt
            # sum([(0.01*2**n) for n in range(7)]) == 1.27
            #
            # If the replicas haven't successfully broadcast the previous
            # request after 1.27sec then there's a bug, and the test fails with
            # "too many attempts".

            for attempt in range(retries):
                response = requests.put('http://{}:{}/kvs/{}'.format(hostname, instance.published_port, key),
                    json={'value':value, 'causal-metadata':self.causal_metadata['metadata']})
                print('Try {attempt}/{retries} PUT {key}:{value} -> {instance} -> {code} @{m}'.format(key=key, value=value, instance=instance, m=self.causal_metadata['metadata'], attempt=attempt + 1, retries=retries, code=response.status_code))
                if response.status_code == 503:
                    sleep(backoffSec(attempt))
                    continue # retry
                else:
                    #print("causal metadata", response.json()['causal-metadata'])
                    self.assertEqual(response.status_code, 201)
                    self.causal_metadata['metadata'] = response.json()['causal-metadata']
                    break # next request
            else:
                self.fail("too many attempts")

        print('... Wait for replication')
        sleep(5)


    def test_e_get_key_value_operation(self):
        '''Did the replicas store the right data in the previous test?'''

        print('=== Check for correctness of {} key:value pairs in the store.'.format(self.key_count))
        for n in range(self.key_count):

            key = 'key{}'.format(n)
            value = 'value{}'.format(n)
            instance = all_instances[(n + 1) % len(all_instances)] # different instance from the one chosen above
            print('=== Check {key}:{value} at instance {instance}'.format(key=key, value=value, instance=instance))

            response = requests.get('http://{}:{}/kvs/{}'.format(hostname, instance.published_port, key),
                json={'causal-metadata':self.causal_metadata['metadata']})
            print('GET {key} -> {instance} -> {code} @{m}'.format(key=key, instance=instance, m=self.causal_metadata['metadata'], code=response.status_code))
            self.assertEqual(response.status_code, 200)
            self.assertIn('value', response.json())
            self.assertEqual(response.json()['value'], value)
            self.causal_metadata['metadata'] = response.json()['causal-metadata']


    def test_f_shard_key_count(self):

        shard_key_counts = dict()
        for shard_id, members in self.shard_members.items():

            with self.subTest(msg='for shard {}'.format(shard_id)):
                print('... The first instance in shard {} is {}'.format(shard_id, members[0]), end=', ')
                (first_instance,) = [instance for instance in all_instances if instance.socket_address == members[0]]
                print(first_instance)

                print('>>> Get key-count for shard {} from {}'.format(shard_id, first_instance))
                response = requests.get('http://{}:{}/shard/key-count/{}'.format(hostname, first_instance.published_port, shard_id))
                self.assertEqual(response.status_code, 200)
                self.assertIn('shard-key-count', response.json())
                shard_key_counts[shard_id] = response.json()['shard-key-count']
                self.assertGreater(shard_key_counts[shard_id], 1)

            print('=== Check that everybody reports key-count {} for shard {}'.format(shard_key_counts[shard_id], shard_id))
            for instance in all_instances: 
                with self.subTest(msg='for shard {}; at instance {}'.format(shard_id, instance)):
                    response = requests.get('http://{}:{}/shard/key-count/{}'.format(hostname, instance.published_port, shard_id))
                    self.assertEqual(response.status_code, 200)
                    self.assertIn('shard-key-count', response.json())
                    self.assertEqual(shard_key_counts[shard_id], response.json()['shard-key-count'])

        self.assertEqual(sum(shard_key_counts.values()), self.key_count, msg='Sum of key-counts-in-shards must equal total-keys')

        print('=== Check whether keys are distributed almost uniformly')

        equal_share = self.key_count / self.shard_count
        min_share = equal_share * 0.75
        max_share = equal_share * 1.25

        for shard_id, shard_key_count in shard_key_counts.items():
            with self.subTest(msg='for shard {}'.format(shard_id)):
                # min_share < shard_key_count < max_share
                self.assertLess(min_share, shard_key_count           )
                self.assertLess(           shard_key_count, max_share)


    def test_g_add_new_node(self):

        print('>>> Start up {}'.format(grace))
        runInstance(grace, all_instances + [grace])

        print('... Give time to bind ports, update views, etc.')
        sleep(5)

        print('=== Check that everybody sees {} in the view'.format(grace))
        for instance in all_instances:
            with self.subTest(msg='at instance {}'.format(instance)):
                response = requests.get('http://{}:{}/view'.format(hostname, instance.published_port))
                self.assertEqual(response.status_code, 200)
                self.assertIn('view', response.json())
                self.assertIn(grace.socket_address, response.json()['view'])

        assigned_shard = random.choice(self.shard_ids)
        shard_assigner = random.choice(all_instances)

        print('>>> Assign {} to shard {} with a request to {}'.format(grace, assigned_shard, shard_assigner))
        response = requests.put('http://{}:{}/shard/add-member/{}'.format(hostname, shard_assigner.published_port, assigned_shard),
                json={'socket-address': grace.socket_address})
        self.assertEqual(response.status_code, 200)
        del shard_assigner

        print('... Give time to replicate keys.')
        sleep(5)

        print('=== Check the shard-id of {} is {}'.format(grace, assigned_shard))
        response = requests.get('http://{}:{}/shard/node-shard-id'.format(hostname, grace.published_port))
        self.assertEqual(response.status_code, 200)
        self.assertIn('node-shard-id', response.json())
        self.assertEqual(response.json()['node-shard-id'], assigned_shard)

        print('=== Check that {} is in shard {} members at each instance'.format(grace, assigned_shard))
        for instance in all_instances:
            with self.subTest(msg='at instance {}'.format(instance)):
                response = requests.get('http://{}:{}/shard/members/{}'.format(hostname, instance.published_port, assigned_shard))
                self.assertEqual(response.status_code, 200)
                self.assertIn('shard-members', response.json())
                self.assertIn(grace.socket_address, response.json()['shard-members'])

        print('=== Check that all instances agree about shard {} key-count'.format(assigned_shard))

        response = requests.get('http://{}:{}/shard/key-count/{}'.format(hostname, grace.published_port, assigned_shard))
        self.assertEqual(response.status_code, 200)
        self.assertIn('shard-key-count', response.json())
        reported_key_count = response.json()['shard-key-count']

        print('=== Check that everybody reports key-count {} for shard {}'.format(reported_key_count, assigned_shard))
        for instance in all_instances: 
            with self.subTest(msg='at instance {}'.format(instance)):
                response = requests.get('http://{}:{}/shard/key-count/{}'.format(hostname, instance.published_port, assigned_shard))
                self.assertEqual(response.status_code, 200)
                self.assertIn('shard-key-count', response.json())
                self.assertEqual(response.json()['shard-key-count'], reported_key_count)


    # def test_h_impossible_reshard(self):

    #     print('=== Check that an impossible reshard is rejected')
    #     instance = random.choice(all_instances)
    #     response = requests.put('http://{}:{}/shard/reshard'.format(hostname, instance.published_port), json={'shard-count': 10})
    #     self.assertEqual(response.status_code, 400)


    def test_i_possible_reshard(self):

        instance = random.choice(all_instances)
        print('>>> Do a reshard at {}'.format(instance))
        with self.subTest(msg='reshard at {}'.format(instance)):
            response = requests.put('http://{}:{}/shard/reshard'.format(hostname, instance.published_port), json={'shard-count': 3})
            self.assertEqual(response.status_code, 200)

        print('... Wait for keys to rebalance')
        sleep(20)

        instance = random.choice(all_instances)
        print('>>> Get shard-ids from {}'.format(instance))
        with self.subTest(msg='shard-ids from {}'.format(instance)):
            response = requests.get('http://{}:{}/shard/ids'.format(hostname, instance.published_port))
            self.assertEqual(response.status_code, 200)
            shard_ids = response.json()['shard-ids']
            self.assertEqual(len(shard_ids), 3)

        print('>>> Get all shard members')
        shard_members = dict()

        for shard_id in shard_ids:
            instance = random.choice(all_instances)
            print('>>> Get shard {} members from {}'.format(shard_id, instance))
            with self.subTest(msg='shard {} members from {}'.format(shard_id, instance)):
                response = requests.get('http://{}:{}/shard/members/{}'.format(hostname, instance.published_port, shard_id))
                self.assertEqual(response.status_code, 200)
                self.assertIn('shard-members', response.json())
                shard_members[shard_id] = response.json()['shard-members']
                self.assertGreater(len(shard_members[shard_id]), 1)

        print('... Shard members:', shard_members)
        self.assertEqual(sum(len(members) for members in shard_members.values()), len(all_instances) + 1,
                msg='The total number of shard members is the length of all_instances, plus grace')


        print('>>> Check that each node sees itself as part of the correct shard')
        for instance in all_instances:
            with self.subTest(msg='at instance {}'.format(instance)):
                print('>>> Get the node-shard-id from {}'.format(instance))
                response = requests.get('http://{}:{}/shard/node-shard-id'.format(hostname, instance.published_port))
                self.assertEqual(response.status_code, 200)
                self.assertIn('node-shard-id', response.json())
                instance_shard_id = response.json()['node-shard-id']

                print('=== Check that the {}\'s shard id {} fits with previously collected data'.format(instance, instance_shard_id))
                self.assertIn(instance_shard_id, shard_members)
                self.assertIn(instance.socket_address, shard_members[instance_shard_id])


        print('>>> Check that shard key counts have the right total')
        shard_key_counts = dict()
        for shard_id, members in shard_members.items():

            with self.subTest(msg='for shard {}'.format(shard_id)):
                print('... The first instance in shard {} is {}'.format(shard_id, members[0]), end=', ')
                (first_instance,) = [instance for instance in all_instances if instance.socket_address == members[0]]
                print(first_instance)

                print('>>> Get key-count for shard {} from {}'.format(shard_id, first_instance))
                response = requests.get('http://{}:{}/shard/key-count/{}'.format(hostname, first_instance.published_port, shard_id))
                self.assertEqual(response.status_code, 200)
                self.assertIn('shard-key-count', response.json())
                shard_key_counts[shard_id] = response.json()['shard-key-count']
                self.assertGreater(shard_key_counts[shard_id], 1)

        self.assertEqual(sum(shard_key_counts.values()), self.key_count,
                msg='Sum of key-counts-in-shards must equal total-keys')


        print('=== Check whether keys are distributed almost uniformly')

        equal_share = self.key_count / len(shard_ids)
        min_share = equal_share * 0.75
        max_share = equal_share * 1.25

        for shard_id, shard_key_count in shard_key_counts.items():
            with self.subTest(msg='for shard {}'.format(shard_id)):
                # min_share < shard_key_count < max_share
                self.assertLess(min_share, shard_key_count           )
                self.assertLess(           shard_key_count, max_share)


        print('=== Check for correctness of {} key:value pairs in the store.'.format(self.key_count))
        for n in range(self.key_count):

            key = 'key{}'.format(n)
            value = 'value{}'.format(n)
            instance = all_instances[(n + 2) % len(all_instances)] # different instance from the one chosen above
            print('=== Check {key}:{value} at instance {instance}'.format(key=key, value=value, instance=instance))

            response = requests.get('http://{}:{}/kvs/{}'.format(hostname, instance.published_port, key),
                json={'causal-metadata':self.causal_metadata['metadata']})
            print('GET {key} -> {instance} -> {code} @{m}'.format(key=key, instance=instance, m=self.causal_metadata['metadata'], code=response.status_code))
            self.assertEqual(response.status_code, 200)
            self.assertIn('value', response.json())
            self.assertEqual(response.json()['value'], value)
            self.causal_metadata['metadata'] = response.json()['causal-metadata']


if __name__ == '__main__':
    try:
        buildDockerImage()
        unittest.main(verbosity=2)
    except KeyboardInterrupt:
        TestHW4.tearDownClass()
