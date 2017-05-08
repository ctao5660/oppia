// Copyright 2014 The Oppia Authors. All Rights Reserved.
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
 * @fileoverview Unit tests for MultipleChoiceInputCheckerService.
 */

describe('Multiple choice input checker service', function() {
  beforeEach(module('oppia'));

  var AnswerGroupObjectFactory = null, mcics = null;
  var customizationArgs = {"choices":{"value":["<p>Option 1</p>","<p>Option 2</p>","<p>Op 3</p>"]}};
  var answerGroupsFalse = [{"rules":[{"type":"Equals","inputs":{"x":0}},{"type":"Equals","inputs":{"x":2}}],"outcome":{"param_changes":[],"feedback":[""],"dest":"Question 1"},"correct":false}];
  var answerGroupsTrue = [{"rules":[{"type":"Equals","inputs":{"x":0}}],"outcome":{"feedback":[""],"param_changes":[],"dest":"Question 2"},"correct":false}];

  beforeEach(inject(function($injector) {
    AnswerGroupObjectFactory = $injector.get('AnswerGroupObjectFactory');
    mcics = $injector.get('MultipleChoiceInputCheckerService');
  }));


  it('should return false for two Equals rules', function() {
    var x = mcics.isValid(customizationArgs, answerGroupsFalse);
    console.log("Dev110 " + x);
    expect(x).toBe(false);
  });

  
  it('should return true for one Equals rules', function() {
    var x = mcics.isValid(customizationArgs, answerGroupsTrue);
    console.log("Dev110 " + x);
    expect(x).toBe(true);
  });

});
