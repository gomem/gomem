// Copyright 2019 Nick Poorman
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

module github.com/gomem/gomem

go 1.13

require (
	github.com/apache/arrow/go/arrow v0.0.0-20190920001900-00a3c47b1559
	github.com/tidwall/gjson v1.6.0
)

// TODO(nickpoorman): Remove this once this PR is merged: https://github.com/apache/arrow/pull/7670
replace github.com/apache/arrow/go/arrow => github.com/nickpoorman/arrow/go/arrow v0.0.0-20200707213152-f76d691be945
