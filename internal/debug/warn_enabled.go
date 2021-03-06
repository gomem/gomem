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

// +build debug warn

package debug

import (
	"log"
	"os"
)

var (
	warn = log.New(os.Stderr, "(warn) ", log.LstdFlags)
)

func Warn(msg interface{}) {
	warn.Print(msg)
}

func Warnf(format string, v ...interface{}) {
	warn.Printf(format, v...)
}
