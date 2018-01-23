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
 * @fileoverview Service to operate the playback of autogenerated audio
 * using the SpeechSynthesis API.
 */

oppia.factory('AutogeneratedAudioPlayerService', [
  'SpeechSynthesisChunkerService',
  function(SpeechSynthesisChunkerService) {
    var DEFAULT_PLAYBACK_RATE = 0.92;

    // Not all browsers support SpeechSynthesisUtterance, so we need the
    // check to prevent a "SpeechSynthesisUtterance is not defined" error.
    var _speechSynthesis = null;
    var utterance = null;
    if (window.hasOwnProperty('speechSynthesis')) {
      _speechSynthesis = window.speechSynthesis;
      utterance = new SpeechSynthesisUtterance();
    }

    var _play = function(html, language, audioFinishedCallback) {
      SpeechSynthesisChunkerService.cancel();
      message = SpeechSynthesisChunkerService.convertToSpeakableText(html);
      utterance.text = message;
      utterance.lang = language;
      utterance.rate = DEFAULT_PLAYBACK_RATE;
      SpeechSynthesisChunkerService.speak(
        utterance,
        function() {
          audioFinishedCallback();
        }
      );
    };

    return {
      play: function(html, language, audioFinishedCallback) {
        return _play(html, language, audioFinishedCallback);
      },
      cancel: function() {
        SpeechSynthesisChunkerService.cancel();
      },
      isPlaying: function() {
        return _speechSynthesis.speaking;
      }
    };
  }
]);
