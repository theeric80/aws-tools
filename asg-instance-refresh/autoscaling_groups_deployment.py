import argparse
import logging
import time
from contextlib import ExitStack
from dataclasses import dataclass
from functools import cached_property, partial, wraps
from operator import attrgetter
from typing import Union

import boto3

formatter = logging.Formatter(
    fmt='%(levelname)s:%(name)s:%(asctime)s: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S%z')

handler = logging.StreamHandler()
handler.setFormatter(formatter)

#logger = logging.getLogger(__name__)
logger = logging.getLogger('asg.deployment')
logger.addHandler(handler)
logger.setLevel(logging.INFO)


@dataclass(frozen=True)
class Instance:
    id: int
    version: str
    state: str
    protected_from_scale_in: bool


def new_instance(d: dict) -> Instance:
    instance_id = d['InstanceId']
    state = d['LifecycleState']
    version = d['LaunchTemplate']['Version']
    protected_from_scale_in = d.get('ProtectedFromScaleIn', False)
    return Instance(instance_id, version, state, protected_from_scale_in)


def wait_with_timeout(timeout: int):

    def decorate(fn):

        @wraps(fn)
        def wrapper(*args, **kwargs):
            step_sec = 30
            for c in range(int(timeout * 60 / step_sec) + 1):
                ready, instances = fn(*args, **kwargs)
                if ready:
                    return instances
                else:
                    for i in sorted(instances, key=attrgetter('version')):
                        logger.debug(
                            f'[{c}] instance {i.id} [{i.version}] in [{i.state}]'
                        )
                    time.sleep(step_sec)
            raise TimeoutError

        return wrapper

    return decorate


class LaunchTemplate:

    def __init__(self, aws_region: str, lt_name: str):
        self._aws_region = aws_region
        self._lt_name = lt_name
        self._client = None

    @property
    def client(self):
        if not self._client:
            self._client = boto3.client('ec2', region_name=self._aws_region)
        return self._client

    def default_version(self) -> str:
        response = self.client.describe_launch_template_versions(
            LaunchTemplateName=self._lt_name, Versions=['$Default'])

        lt = response['LaunchTemplateVersions'][0]
        return str(lt['VersionNumber'])

    def latest_version(self) -> str:
        response = self.client.describe_launch_template_versions(
            LaunchTemplateName=self._lt_name, Versions=['$Latest'])

        lt = response['LaunchTemplateVersions'][0]
        return str(lt['VersionNumber'])

    def update_default_version(self, version: str) -> str:
        logger.info(f'update_default_version: {version}')

        response = self.client.modify_launch_template(
            LaunchTemplateName=self._lt_name, DefaultVersion=version)

        lt = response['LaunchTemplate']
        return str(lt['DefaultVersionNumber'])


class AutoScalingGroup:

    def __init__(self, aws_region: str, asg_name: str):
        self._aws_region = aws_region
        self._asg_name = asg_name
        self._client = None
        self._lt = None

    @property
    def client(self):
        if not self._client:
            self._client = boto3.client('autoscaling',
                                        region_name=self._aws_region)
        return self._client

    @property
    def launch_template(self):
        if not self._lt:
            response = self.client.describe_auto_scaling_groups(
                AutoScalingGroupNames=[self._asg_name])

            lt = response['AutoScalingGroups'][0]['LaunchTemplate']
            self._lt = LaunchTemplate(self._aws_region,
                                      lt['LaunchTemplateName'])
        return self._lt

    def is_updated(self, version: str) -> bool:
        instances = self.instances() + self.warm_pool_instances()

        updated = all(i.version == version for i in instances)
        return updated

    def launch_template_version(self) -> str:
        response = self.client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[self._asg_name])

        lt = response['AutoScalingGroups'][0]['LaunchTemplate']
        lt_version = lt['Version']

        if lt_version == '$Default':
            return self.launch_template.default_version()
        elif lt_version == '$Latest':
            return self.launch_template.latest_version()
        else:
            return lt_version

    @cached_property
    def warm_pool_created(self) -> bool:
        response = self.client.describe_warm_pool(
            AutoScalingGroupName=self._asg_name)
        return 'WarmPoolConfiguration' in response

    def group_size(self) -> Union[int, int, int]:
        response = self.client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[self._asg_name])

        assert (len(response['AutoScalingGroups']) > 0)

        asg = response['AutoScalingGroups'][0]
        min_size = asg['MinSize']
        max_size = asg['MaxSize']
        desired_capacity = asg['DesiredCapacity']
        return desired_capacity, min_size, max_size

    def instances(self) -> list[Instance]:
        response = self.client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[self._asg_name])

        asg = response['AutoScalingGroups'][0]
        asg_instances = asg['Instances']
        return [new_instance(i) for i in asg_instances]

    def warm_pool_instances(self) -> list[Instance]:
        response = self.client.describe_warm_pool(
            AutoScalingGroupName=self._asg_name)

        asg_instances = response['Instances']
        return [new_instance(i) for i in asg_instances]

    @wait_with_timeout(60)
    def wait_group_size_until(self, size: int) -> Union[bool, list[Instance]]:
        logger.info(f'wait_group_size_until: {size}')

        asg_instances = self.instances() + self.warm_pool_instances()
        ready = len(asg_instances) == size
        return ready, asg_instances

    @wait_with_timeout(60)
    def wait_instance_size_until(self,
                                 size: int) -> Union[bool, list[Instance]]:
        logger.info(f'wait_instance_size_until: {size}')

        asg_instances = self.instances()
        ready = len(asg_instances) == size
        return ready, asg_instances

    @wait_with_timeout(60)
    def wait_warm_pool_size_until(self,
                                  size: int) -> Union[bool, list[Instance]]:
        logger.info(f'wait_warm_pool_size_until: {size}')

        asg_instances = self.warm_pool_instances()
        ready = len(asg_instances) == size
        return ready, asg_instances

    @wait_with_timeout(60)
    def wait_instances_for(self,
                           state: list[str]) -> Union[bool, list[Instance]]:
        logger.info(f'wait_instances_for: {state}')

        asg_instances = self.instances()
        ready = all(i.state in state for i in asg_instances)
        return ready, asg_instances

    @wait_with_timeout(60)
    def wait_warm_pool_for(self,
                           state: list[str]) -> Union[bool, list[Instance]]:
        logger.info(f'wait_warm_pool_for: {state}')

        asg_instances = self.warm_pool_instances()
        ready = all(i.state in state for i in asg_instances)
        return ready, asg_instances

    @wait_with_timeout(60)
    def wait_instance_refresh_completion(
            self, instance_refresh_id: str) -> Union[bool, list]:
        logger.info(f'wait_instance_refresh_completion: {instance_refresh_id}')

        response = self.client.describe_instance_refreshes(
            AutoScalingGroupName=self._asg_name,
            InstanceRefreshIds=[instance_refresh_id])

        instance_refresh = response['InstanceRefreshes'][0]
        status = instance_refresh['Status']
        expected_status = ( \
                'successful', 'failed', 'cancelled', 'rollbackfailed', 'rollbacksuccessful')
        ready = status.lower() in expected_status

        if not ready:
            percentage = instance_refresh.get('PercentageComplete', 0)
            logger.info(
                f'wait_instance_refresh_completion: {status}, {percentage}')

        return ready, []

    def suspend_processes(self, processes: list[str]):
        logger.info(f'suspend_processes: {processes}')

    def resume_processes(self, processes: list[str]):
        logger.info(f'resume_processes: {processes}')

    def remove_instance_protection(self, instances: list[Instance]):
        instance_ids = [i.id for i in instances if i.protected_from_scale_in]
        logger.info(f'remove_instance_protection: {instance_ids}')

        self.client.set_instance_protection(InstanceIds=instance_ids,
                                            AutoScalingGroupName=self._asg_name,
                                            ProtectedFromScaleIn=False)

    def update_launch_template_version(self, version: str) -> str:
        response = self.client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[self._asg_name])

        lt = response['AutoScalingGroups'][0]['LaunchTemplate']
        lt_name, lt_version = lt['LaunchTemplateName'], lt['Version']

        logger.info(
            f'update_launch_template_version: {lt_version} >> {version}')

        if lt_version == '$Default':
            if version != self.launch_template.default_version():
                self.launch_template.update_default_version(version)
            return version
        elif lt_version == '$Latest':
            return self.launch_template.latest_version()
        else:
            if version != lt_version:
                kwargs = dict()
                kwargs['AutoScalingGroupName'] = self._asg_name
                kwargs['LaunchTemplate'] = {
                    'LaunchTemplateName': lt_name,
                    'Version': version
                }
                logger.info(f'update_group_version: {kwargs}')
                self.client.update_auto_scaling_group(**kwargs)
            return version

    def update_group_size(self,
                          max_size: int = None,
                          min_size: int = None,
                          desired_capacity: int = None):
        if all(sz is None for sz in (max_size, min_size, desired_capacity)):
            return

        kwargs = dict()
        kwargs['AutoScalingGroupName'] = self._asg_name

        if max_size is not None:
            kwargs['MaxSize'] = max_size
        if min_size is not None:
            kwargs['MinSize'] = min_size
        if desired_capacity is not None:
            kwargs['DesiredCapacity'] = desired_capacity

        logger.info(f'update_group_size: {kwargs}')

        self.client.update_auto_scaling_group(**kwargs)

    def refresh_instance(self, version: str = None):
        logger.info(f'start to refresh autoscaling group: {self._asg_name}')

        if version is None:
            lt_version = self.launch_template_version()
        else:
            lt_version = self.update_launch_template_version(version)
        updated = self.is_updated(lt_version)

        logger.info(f'version to update: {lt_version}, updated: {updated}')

        if updated:
            return

        if self.warm_pool_created:
            self.start_blue_green_deployment()
        else:
            self.start_rolling_update()

        assert (self.is_updated(lt_version))

    def start_rolling_update(self):
        logger.info(f'start_rolling_update ...')

        preferences = {'MinHealthyPercentage': 90}
        response = self.client.start_instance_refresh(
            AutoScalingGroupName=self._asg_name,
            Strategy='Rolling',
            Preferences=preferences)

        instance_refresh_id = response['InstanceRefreshId']
        self.wait_instance_refresh_completion(instance_refresh_id)

    def start_blue_green_deployment(self):
        logger.info(f'start_blue_green_deployment ...')
        assert (self.warm_pool_created)

        # TODO: insufficient capacity

        init_desired_capacity, init_min_size, init_max_size = self.group_size()
        logger.info(f'init desired_capacity: {init_desired_capacity}')
        logger.info(f'init min_size: {init_min_size}')
        logger.info(f'init max_size: {init_max_size}')

        with ExitStack() as stack:
            # step 0: recover max_size and min_size when a deployment fails
            stack.callback(
                partial(self.update_group_size,
                        max_size=init_max_size,
                        min_size=init_min_size))

            # step 1: launch instances with new launch template
            logger.info(f'===== instance refresh. step: 1 =====')
            max_size = 2 * init_max_size
            self.update_group_size(max_size=max_size)

            self.wait_group_size_until(max_size)
            self.wait_warm_pool_for(['Warmed:Stopped'])

            # step 1.5: pause scaling processes
            scaling_processes = [
                'AlarmNotification', 'AZRebalance', 'InstanceRefresh'
            ]
            self.suspend_processes(scaling_processes)
            stack.callback(partial(self.resume_processes, scaling_processes))

            # step 2: terminate old instances in warm pool
            logger.info(f'===== instance refresh. step: 2 =====')
            max_size = init_max_size + init_desired_capacity
            self.update_group_size(max_size=max_size)

            self.wait_group_size_until(max_size)
            self.wait_warm_pool_for(['Warmed:Stopped'])

            # step 3: add new instances into main group
            logger.info(f'===== instance refresh. step: 3 =====')
            min_size = 2 * init_desired_capacity
            self.update_group_size(min_size=min_size)

            self.wait_instance_size_until(min_size)
            self.wait_instances_for(['InService'])

            # step 4: remove scale-in protection
            logger.info(f'===== instance refresh. step: 4 =====')
            self.remove_instance_protection(self.instances())

            # step 5: remove old instances from main group
            logger.info(f'===== instance refresh. step: 5 =====')
            self.update_group_size(min_size=init_min_size,
                                   desired_capacity=init_desired_capacity)

            self.wait_instance_size_until(init_desired_capacity)
            self.wait_warm_pool_for(['Warmed:Stopped'])

            # step 6: terminate old instances in warm pool
            logger.info(f'===== instance refresh. step: 6 =====')
            self.update_group_size(max_size=init_max_size)
            self.wait_group_size_until(init_max_size)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('region', help='The region to use.')
    parser.add_argument('asg_name', help='The name of the Auto Scaling group.')
    parser.add_argument('--version',
                        help='The version number of the launch template.',
                        type=str)
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    asg = AutoScalingGroup(args.region, args.asg_name)
    asg.refresh_instance(args.version)


main()
