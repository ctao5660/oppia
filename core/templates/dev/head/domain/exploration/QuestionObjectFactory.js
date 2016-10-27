// Copyright 2015 The Oppia Authors. All Rights Reserved.
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
 * @fileoverview Factory for creating new frontend instances of Question
 * domain objects.
 */

oppia.factory('QuestionObjectFactory', [function() {
  var Question = function(
      stateName, interactionId, interactionCustomizationArgs, answerGroups,
      defaultOutcome, bridgeHtml) {
    this._stateName = stateName;
    this._interactionId = interactionId;
    this._interactionCustomizationArgs = interactionCustomizationArgs;
    this._answerGroups = answerGroups;
    this._defaultOutcome = defaultOutcome;
    this._bridgeHtml = bridgeHtml;
  };

  // Instance methods.
  Question.prototype.getStateName = function() {
    return this._stateName;
  };

  Question.prototype.getInteractionId = function() {
    return this._interactionId;
  };

  Question.prototype.getInteractionCustomizationArgs = function() {
    return this._interactionCustomizationArgs;
  };

  Question.prototype.getAnswerGroups = function() {
    return this._answerGroups;
  };

  Question.prototype.getDefaultOutcome = function() {
    return this._defaultOutcome;
  };

  Question.prototype.getDestinationStateName = function() {
    return this._answerGroups[0].outcome.dest;
  };

  Question.prototype.getBridgeHtml = function() {
    return this._bridgeHtml;
  };

  Question.prototype.setInteractionCustomizationArgs = function(
      newCustomizationArgs) {
    this._interactionCustomizationArgs = newCustomizationArgs;
  };

  Question.prototype.setAnswerGroups = function(newAnswerGroups) {
    this._answerGroups = newAnswerGroups;
  };

  Question.prototype.setDefaultOutcome = function(newDefaultOutcome) {
    this._defaultOutcome = newDefaultOutcome;
  };

  Question.prototype.setBridgeHtml = function(newHtml) {
    this._bridgeHtml = newHtml;
  };

  Question.prototype.hasAnswerGroups = function() {
    return this._answerGroups.length > 0;
  };

  // Static class methods. Note that "this" is not available in
  // static contexts.
  Question.create = function(stateName, interactionDict, bridgeHtml) {
    var copyOfInteractionDict = angular.copy(interactionDict);

    return new Question(
      stateName,
      copyOfInteractionDict.id,
      copyOfInteractionDict.customization_args,
      copyOfInteractionDict.answer_groups,
      copyOfInteractionDict.default_outcome,
      bridgeHtml);
  };

  return Question;
}]);
