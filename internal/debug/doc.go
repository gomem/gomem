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

/*
Package debug provides compiled assertions, debug and warn level logging.

To enable runtime debug or warn level logging, build with the debug or warn tags
respectively. Building with the debug tag will enable the warn level logger automatically.
When the debug and warn tags are omitted, the code for the logging will be ommitted from
the binary.

To enable runtime assertions, build with the assert tag. When the assert tag is omitted,
the code for the assertions will be ommitted from the binary.
*/
package debug
