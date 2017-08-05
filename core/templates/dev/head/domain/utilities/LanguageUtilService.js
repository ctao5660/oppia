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
 * @fileoverview Utility service for language operations.
 */

oppia.factory('LanguageUtilService', [function() {
  var supportedAudioLanguages = constants.SUPPORTED_AUDIO_LANGUAGES;

  var allAudioLanguageCodes = (
    supportedAudioLanguages.map(function(audioLanguage) {
      return audioLanguage.id;
    }));
  var audioLanguagesCount = allAudioLanguageCodes.length;

  var audioLanguageCodesToDescriptions = {};
  supportedAudioLanguages.forEach(function(audioLanguage) {
    audioLanguageCodesToDescriptions[audioLanguage.id] = audioLanguage.text;
  });

  return {
    getAudioLanguagesCount: function() {
      return audioLanguagesCount;
    },
    getAudioLanguageDescription: function(audioLanguageCode) {
      return audioLanguageCodesToDescriptions[audioLanguageCode];
    },
    // Given a list of audio language codes, returns the complement list, i.e.
    // the list of audio language codes not in the input list.
    getComplementAudioLanguageCodes: function(audioLanguageCodes) {
      return allAudioLanguageCodes.filter(function(languageCode) {
        return audioLanguageCodes.indexOf(languageCode) === -1;
      });
    }
  }
}]);
