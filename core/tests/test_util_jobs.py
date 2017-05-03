# coding: utf-8
#
# Copyright 2014 The Oppia Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Jobs operating on explorations that can be used for production tests.

To use these jobs, the developer should register them in jobs_registry. (At
the moment, they are not displayed there, in order to avoid accidental use).
"""

from core import jobs
from core.domain import exp_domain
from core.domain import exp_services
from core.domain import rights_manager
from core.platform import models
import feconf

(base_models, exp_models,) = models.Registry.import_models([
    models.NAMES.base_model, models.NAMES.exploration])


class ExpCopiesOneOffJob(jobs.BaseMapReduceJobManager):
    """A one-off job that creates 10 published copies of every existing
    published exploration, with the eid being '[old_eid]copy[copy_number]',
    title 'Copy' and category 'Copies'.
    """
    @classmethod
    def entity_classes_to_map_over(cls):
        return [exp_models.ExplorationModel]

    @staticmethod
    def map(item):
        if ExpCopiesOneOffJob._entity_created_before_job_queued(item):
            exp_rights = rights_manager.get_exploration_rights(item.id)
            if exp_rights.status == rights_manager.ACTIVITY_STATUS_PRIVATE:
                return

            for count in range(10):
                exploration = exp_services.get_exploration_from_model(item)
                exploration.title = 'Copy'
                exploration.category = 'Copies'
                yield ('%scopy%d' % (item.id, count), exploration.to_yaml())

    @staticmethod
    def reduce(exp_id, list_of_exps):
        for stringified_exp in list_of_exps:
            exploration = exp_domain.Exploration.from_yaml(
                exp_id, stringified_exp)
            exp_services.save_new_exploration(
                feconf.SYSTEM_COMMITTER_ID, exploration)
            rights_manager.publish_exploration(
                feconf.SYSTEM_COMMITTER_ID, exp_id)


class DeleteExpCopiesOneOffJob(jobs.BaseMapReduceJobManager):
    """A one-off job that deletes all explorations in the category 'Copies'."""
    @classmethod
    def entity_classes_to_map_over(cls):
        return [exp_models.ExplorationModel]

    @staticmethod
    def map(item):
        if item.category == 'Copies':
            exp_services.delete_exploration(
                feconf.SYSTEM_COMMITTER_ID, item.id, force_deletion=True)

    @staticmethod
    def reduce(exp_id, list_of_exps):
        pass
