/*
 * @Author: "xieyuhua" "1130
 * @Date: 2023-04-21 15:11:48
 * @LastEditors: "xieyuhua" "1130
 * @LastEditTime: 2024-03-02 15:09:12
 * @FilePath: \LogMiner\module\migrate\interf.go
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package migrate

type Extractor interface {
	GetTableRows() ([]string, []string, error)
}

type Translator interface {
	TranslateTableRows() error
}

type Applier interface {
	ApplyTableRows() error
}

type Fuller interface {
	Full() error
}

type Increr interface {
	Incr() error
	Full() error
	FullIncr() error
}
