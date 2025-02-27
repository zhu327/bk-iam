/*
 * TencentBlueKing is pleased to support the open source community by making 蓝鲸智云-权限中心(BlueKing-IAM) available.
 * Copyright (C) 2017-2021 THL A29 Limited, a Tencent company. All rights reserved.
 * Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://opensource.org/licenses/MIT
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package condition

import (
	"errors"

	. "github.com/onsi/ginkgo"
	"github.com/stretchr/testify/assert"
)

type ctx int

func (c ctx) GetAttr(key string) (interface{}, error) {
	return int(c), nil
}

func (c ctx) GetFullNameAttr(key string) (interface{}, error) {
	return "test", nil
}

type strCtx string

func (c strCtx) GetAttr(key string) (interface{}, error) {
	return string(c), nil
}

func (c strCtx) GetFullNameAttr(key string) (interface{}, error) {
	return "test", nil
}

type boolCtx bool

func (c boolCtx) GetAttr(key string) (interface{}, error) {
	return bool(c), nil
}

func (c boolCtx) GetFullNameAttr(key string) (interface{}, error) {
	return "test", nil
}

type listCtx []interface{}

func (c listCtx) GetAttr(key string) (interface{}, error) {
	x := []interface{}(c)
	return x, nil
}

func (c listCtx) GetFullNameAttr(key string) (interface{}, error) {
	return "test", nil
}

type errCtx int

func (c errCtx) GetAttr(key string) (interface{}, error) {
	return nil, errors.New("missing key")
}

func (c errCtx) GetFullNameAttr(key string) (interface{}, error) {
	return nil, errors.New("missing key")
}

var _ = Describe("BaseCondition", func() {

	Describe("GetValues", func() {

		It("ok", func() {
			expectedValues := []interface{}{1, "ab", 3}
			c := baseCondition{
				Key:   "test",
				Value: expectedValues,
			}
			assert.Equal(GinkgoT(), expectedValues, c.GetValues())
		})
	})

	Describe("forOr", func() {
		var fn func(interface{}, interface{}) bool
		var condition *baseCondition
		BeforeEach(func() {
			fn = func(a interface{}, b interface{}) bool {
				return a == b
			}
			condition = &baseCondition{
				Key: "key",
				Value: []interface{}{
					1,
					2,
				},
			}
		})

		It("GetAttr fail", func() {
			allowed := condition.forOr(errCtx(1), fn)
			assert.False(GinkgoT(), allowed)
		})

		It("single, hit one", func() {
			assert.True(GinkgoT(), condition.forOr(ctx(1), fn))
			assert.True(GinkgoT(), condition.forOr(ctx(2), fn))
		})
		It("single, missing ", func() {
			assert.False(GinkgoT(), condition.forOr(ctx(3), fn))
		})

		It("list, hit one", func() {
			assert.True(GinkgoT(), condition.forOr(listCtx{2, 3}, fn))
		})
		It("list, missing", func() {
			assert.False(GinkgoT(), condition.forOr(listCtx{3, 4}, fn))
		})

	})
	Describe("GetKeys", func() {
		It("ok", func() {
			expectedKey := "test"

			c := baseCondition{
				Key:   expectedKey,
				Value: nil,
			}
			assert.Equal(GinkgoT(), []string{expectedKey}, c.GetKeys())
		})

	})

})
