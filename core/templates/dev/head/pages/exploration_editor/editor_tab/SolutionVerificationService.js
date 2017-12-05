// Copyright 2017 The Oppia Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS-IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * @fileoverview Service for solution verification.
 */

oppia.factory('SolutionVerificationService', [
  '$injector', 'AngularNameService', 'AnswerClassificationService',
  function(
    $injector, AngularNameService, AnswerClassificationService) {
    return {
      verifySolution: function(explorationId, state, correctAnswer) {
        var interactionId = state.interaction.id;
        var rulesServiceName = (
          AngularNameService.getNameOfInteractionRulesService(interactionId));
        var rulesService = $injector.get(rulesServiceName);
        var result = (
          AnswerClassificationService.getMatchingClassificationResult(
            explorationId, state.name, state, correctAnswer, true, rulesService
          ));
        return state.name !== result.outcome.dest;
      }
    }
  }]);
