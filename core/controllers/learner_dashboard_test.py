# Copyright 2017 The Oppia Authors. All Rights Reserved.
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

"""Tests for the learner dashboard and the notifications dashboard."""

from core.domain import exp_services
from core.domain import learner_progress_services
from core.domain import subscription_services
from core.tests import test_utils
import feconf

class LearnerDashboardHandlerTest(test_utils.GenericTestBase):

    OWNER_EMAIL = 'owner@example.com'
    OWNER_USERNAME = 'owner'

    EXP_ID = 'exp_id'
    EXP_TITLE = 'Exploration title'

    COL_ID = 'col_id'
    COL_TITLE = 'Collection title'

    def setUp(self):
        super(LearnerDashboardHandlerTest, self).setUp()
        self.signup(self.OWNER_EMAIL, self.OWNER_USERNAME)
        self.signup(self.VIEWER_EMAIL, self.VIEWER_USERNAME)

        self.viewer_id = self.get_user_id_from_email(self.VIEWER_EMAIL)
        self.owner_id = self.get_user_id_from_email(self.OWNER_EMAIL)

    def test_can_see_completed_explorations(self):
        self.login(self.VIEWER_EMAIL)

        response = self.get_json(feconf.LEARNER_DASHBOARD_DATA_URL)
        self.assertEqual(len(response['completed_explorations_list']), 0)

        self.save_new_default_exploration(
            self.EXP_ID, self.owner_id, title=self.EXP_TITLE)

        learner_progress_services.mark_exploration_as_completed(
            self.viewer_id, self.EXP_ID)
        response = self.get_json(feconf.LEARNER_DASHBOARD_DATA_URL)
        self.assertEqual(len(response['completed_explorations_list']), 1)
        self.assertEqual(
            response['completed_explorations_list'][0]['id'], self.EXP_ID)
        self.logout()

    def test_can_see_completed_collections(self):
        self.login(self.VIEWER_EMAIL)

        response = self.get_json(feconf.LEARNER_DASHBOARD_DATA_URL)
        self.assertEqual(len(response['completed_collections_list']), 0)

        self.save_new_default_collection(
            self.COL_ID, self.owner_id, title=self.COL_TITLE)

        learner_progress_services.mark_collection_as_completed(
            self.viewer_id, self.COL_ID)
        response = self.get_json(feconf.LEARNER_DASHBOARD_DATA_URL)
        self.assertEqual(len(response['completed_collections_list']), 1)
        self.assertEqual(
            response['completed_collections_list'][0]['id'], self.COL_ID)
        self.logout()

    def test_can_see_incomplete_explorations(self):
        self.login(self.VIEWER_EMAIL)

        response = self.get_json(feconf.LEARNER_DASHBOARD_DATA_URL)
        self.assertEqual(len(response['incomplete_explorations_list']), 0)

        self.save_new_default_exploration(
            self.EXP_ID, self.owner_id, title=self.EXP_TITLE)

        state_name = 'state_name'
        version = 1

        learner_progress_services.mark_exploration_as_incomplete(
            self.viewer_id, self.EXP_ID, state_name, version)
        response = self.get_json(feconf.LEARNER_DASHBOARD_DATA_URL)
        self.assertEqual(len(response['incomplete_explorations_list']), 1)
        self.assertEqual(
            response['incomplete_explorations_list'][0]['id'], self.EXP_ID)
        self.logout()

    def test_can_see_incomplete_collections(self):
        self.login(self.VIEWER_EMAIL)

        response = self.get_json(feconf.LEARNER_DASHBOARD_DATA_URL)
        self.assertEqual(len(response['incomplete_collections_list']), 0)

        self.save_new_default_collection(
            self.COL_ID, self.owner_id, title=self.COL_TITLE)

        learner_progress_services.mark_collection_as_incomplete(
            self.viewer_id, self.COL_ID)
        response = self.get_json(feconf.LEARNER_DASHBOARD_DATA_URL)
        self.assertEqual(len(response['incomplete_collections_list']), 1)
        self.assertEqual(
            response['incomplete_collections_list'][0]['id'], self.COL_ID)
        self.logout()

    def test_can_see_exploration_playlist(self):
        self.login(self.VIEWER_EMAIL)

        response = self.get_json(feconf.LEARNER_DASHBOARD_DATA_URL)
        self.assertEqual(len(response['exploration_playlist']), 0)

        self.save_new_default_exploration(
            self.EXP_ID, self.owner_id, title=self.EXP_TITLE)

        learner_progress_services.add_exp_to_learner_playlist(
            self.viewer_id, self.EXP_ID)
        response = self.get_json(feconf.LEARNER_DASHBOARD_DATA_URL)
        self.assertEqual(len(response['exploration_playlist']), 1)
        self.assertEqual(
            response['exploration_playlist'][0]['id'], self.EXP_ID)
        self.logout()

    def test_can_see_collection_playlist(self):
        self.login(self.VIEWER_EMAIL)

        response = self.get_json(feconf.LEARNER_DASHBOARD_DATA_URL)
        self.assertEqual(len(response['collection_playlist']), 0)

        self.save_new_default_collection(
            self.COL_ID, self.owner_id, title=self.COL_TITLE)

        learner_progress_services.add_collection_to_learner_playlist(
            self.viewer_id, self.COL_ID)
        response = self.get_json(feconf.LEARNER_DASHBOARD_DATA_URL)
        self.assertEqual(len(response['collection_playlist']), 1)
        self.assertEqual(
            response['collection_playlist'][0]['id'], self.COL_ID)
        self.logout()

    def test_can_see_subscription(self):
        self.login(self.VIEWER_EMAIL)

        response = self.get_json(feconf.LEARNER_DASHBOARD_DATA_URL)
        self.assertEqual(len(response['subscription_list']), 0)

        subscription_services.subscribe_to_creator(
            self.viewer_id, self.owner_id)
        response = self.get_json(feconf.LEARNER_DASHBOARD_DATA_URL)
        self.assertEqual(len(response['subscription_list']), 1)
        self.assertEqual(
            response['subscription_list'][0]['creator_username'],
            self.OWNER_USERNAME)
        self.logout()


class LearnerDashboardFeedbackThreadHandlerTest(test_utils.GenericTestBase):

    EXP_ID = '0'

    def setUp(self):
        super(LearnerDashboardFeedbackThreadHandlerTest, self).setUp()
        self.signup(self.EDITOR_EMAIL, self.EDITOR_USERNAME)

        # Load exploration 0.
        exp_services.load_demo(self.EXP_ID)

        # Get the CSRF token and create a single thread with a single message.
        self.login(self.EDITOR_EMAIL)
        response = self.testapp.get('/create/%s' % self.EXP_ID)
        self.csrf_token = self.get_csrf_token_from_response(response)
        self.post_json('%s/%s' % (
            feconf.FEEDBACK_THREADLIST_URL_PREFIX, self.EXP_ID
        ), {
            'state_name': self._get_unicode_test_string('statename'),
            'subject': self._get_unicode_test_string('subject'),
            'text': 'a sample message',
        }, self.csrf_token)
        self.logout()

    def test_get_message_summaries(self):
        self.login(self.EDITOR_EMAIL)
        # Fetch all the feedback threads of that exploration.
        response_dict = self.get_json(
            '%s/%s' % (feconf.FEEDBACK_THREADLIST_URL_PREFIX, self.EXP_ID))

        # Get the id of the thread.
        thread_id = response_dict['threads'][0]['thread_id']

        # Get the message summary of the thread.
        thread_url = '%s/%s/%s' % (
            feconf.LEARNER_DASHBOARD_FEEDBACK_THREAD_DATA_URL, self.EXP_ID,
            thread_id)
        response_dict = self.get_json(thread_url)
        messages_summary = response_dict['message_summary_list']
        first_message = messages_summary[0]

        self.assertDictContainsSubset({
            'text': 'a sample message',
            'author_username': 'editor'
        }, first_message)

        # Add another message.
        thread_url = '%s/%s/%s' % (
            feconf.FEEDBACK_THREAD_URL_PREFIX, self.EXP_ID, thread_id)
        self.post_json(thread_url, {
            'updated_status': None,
            'updated_subject': None,
            'text': 'Message 1'
        }, self.csrf_token)

        # Again fetch the thread message summary.
        thread_url = '%s/%s/%s' % (
            feconf.LEARNER_DASHBOARD_FEEDBACK_THREAD_DATA_URL, self.EXP_ID,
            thread_id)
        response_dict = self.get_json(thread_url)
        messages_summary = response_dict['message_summary_list']

        # Check the summary of the second message.
        second_message = messages_summary[1]
        self.assertDictContainsSubset({
            'text': 'Message 1',
            'author_username': 'editor'
        }, second_message)

        self.logout()
