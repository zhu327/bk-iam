/*
 * TencentBlueKing is pleased to support the open source community by making 蓝鲸智云-权限中心(BlueKing-IAM) available.
 * Copyright (C) 2017-2021 THL A29 Limited, a Tencent company. All rights reserved.
 * Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://opensource.org/licenses/MIT
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package pdp

import (
	"database/sql"
	"errors"

	"iam/pkg/abac/pdp/evaluation"
	pdptypes "iam/pkg/abac/pdp/types"
	"iam/pkg/abac/pip"
	"iam/pkg/abac/prp"
	"iam/pkg/abac/types"
	"iam/pkg/abac/types/request"
	"iam/pkg/errorx"
	"iam/pkg/logging/debug"
)

// PDPHelper ...
const PDPHelper = "PDPHelper"

// ErrNoPolicies ...
var (
	ErrNoPolicies       = errors.New("no policies")
	ErrInvalidAction    = errors.New("action.id invalid")
	ErrSubjectNotExists = errors.New("subject not exists")
)

func queryPolicies(
	system string,
	subject types.Subject,
	action types.Action,
	withoutCache bool,
	entry *debug.Entry,
) (policies []types.AuthPolicy, err error) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf(PDPHelper, "queryPolicies")

	manager := prp.NewPolicyManager()

	policies, err = manager.ListBySubjectAction(system, subject, action, withoutCache, entry)
	if err != nil {
		err = errorWrapf(err,
			"ListBySubjectAction system=`%s`, subject=`%s`, action=`%s`, withoutCache=`%t` fail",
			system, subject, action, withoutCache)
		return
	}

	// 如果没有策略, 直接返回 false
	if len(policies) == 0 {
		err = ErrNoPolicies
		return
	}

	return
}

func filterPoliciesByEvalResources(
	r *request.Request,
	policies []types.AuthPolicy,
) (filteredPolicies []types.AuthPolicy, err error) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf(PDPHelper, "filterPoliciesByEvalResources")

	// 问题: 一次性取? 还是计算一个取一个?
	// NOTE: 重要, 这个需要处理, 以降低影响?
	// 问题: 第三方系统查询不到, policy列表 和 auth鉴权结果怎么返回? 鉴权false? policy列表直接不过滤全返回?
	// if contains remote Resource
	if r.HasRemoteResources() {
		err = fillRemoteResourceAttrs(r, policies)
		if err != nil {
			return nil, errorWrapf(err, "fillRemoteResourceAttrs fail", "")
		}
	}

	// get local + remote resources
	resources := r.GetSortedResources()
	for _, resource := range resources {
		ctx := pdptypes.NewExprContext(r, resource)

		// 10. PDP遍历计算依赖resource的属性是否满足policies
		policies, err = evaluation.FilterPolicies(ctx, policies)
		if err != nil {
			err = errorWrapf(err, "evaluation.FilterPolicies resource=`%+v`, policies=`%+v` fail",
				resource, policies)
			return
		}

		if len(policies) == 0 {
			err = ErrNoPolicies
			return
		}
	}

	filteredPolicies = policies
	return filteredPolicies, nil
}

// queryFilterPolicies 查询请求相关的Policy
func queryFilterPolicies(
	r *request.Request,
	entry *debug.Entry,
	willCheckRemoteResource, // 是否检查请求的外部依赖资源完成性
	withoutCache bool,
) ([]types.AuthPolicy, error) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf(PDP, "queryFilterPolicies")

	// init debug entry with values
	if entry != nil {
		debug.WithValues(entry, map[string]interface{}{
			"system":       r.System,
			"subject":      r.Subject,
			"action":       r.Action,
			"resources":    r.Resources,
			"cacheEnabled": !withoutCache,
		})
	}

	// 1. PIP查询action的scop
	debug.AddStep(entry, "Fetch action details")
	err := fillActionDetail(r)
	if err != nil {
		err = errorWrapf(err, "Fetch action detail action=`%+v` fail", r.Action)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrInvalidAction
		}

		return nil, err
	}
	debug.WithValue(entry, "action", r.Action)

	if willCheckRemoteResource {
		// 2. 检查请求资源与action关联的外部依赖资源类型是否匹配
		debug.AddStep(entry, "Validate action remote resource")
		if !r.ValidateActionRemoteResource() {
			err = errorWrapf(ErrInvalidActionResource,
				"ValidateActionResource systemID=`%s`, actionID=`%d`, resources=`%+v` fail, "+
					"request resources not match action",
				r.System, r.Action.ID, r.Resources)

			return nil, err
		}
	}

	// 3. PIP查询subject相关的属性
	debug.AddStep(entry, "Fetch subject details")
	err = fillSubjectDetail(r)
	if err != nil {
		// 如果用户不存在, 表现为没有权限
		// if the subject not exists
		if errors.Is(err, sql.ErrNoRows) {
			return []types.AuthPolicy{}, nil
		}

		err = errorWrapf(err, "request fillSubjectDetail subject=`%+v`", r.Subject)
		return nil, err
	}
	debug.WithValue(entry, "subject", r.Subject)

	// 4. PRP查询subject-action相关的policies
	debug.AddStep(entry, "Query Policies")
	policies, err := queryPolicies(r.System, r.Subject, r.Action, withoutCache, entry)
	if err != nil {
		if errors.Is(err, ErrNoPolicies) {
			return nil, nil
		}

		err = errorWrapf(err, "queryPolicies system=`%s`, subject=`%+v`, action=`%+v`, withoutCache=`%t` fail",
			r.System, r.Subject, r.Action, withoutCache)

		return nil, err
	}
	debug.WithValue(entry, "policies", policies)
	debug.WithUnknownEvalPolicies(entry, policies)

	// 5. filter policies
	// 这里需要返回剩下的policies
	debug.AddStep(entry, "Filter policies by eval resources")
	var filteredPolicies []types.AuthPolicy
	filteredPolicies, err = filterPoliciesByEvalResources(r, policies)
	if err != nil {
		if errors.Is(err, ErrNoPolicies) {
			// if is len(filteredPolicies) == 0, update all to no pass
			debug.WithNoPassEvalPolicies(entry, policies)

			// if return nil, the condition will be null in response
			return []types.AuthPolicy{}, nil
		}

		err = errorWrapf(err, "filterPoliciesByEvalResources policies=`%+v` fail", policies)

		return nil, err
	}

	// update all  filteredPolicies to pass, 有一条过就算过
	debug.WithPassEvalPolicies(entry, filteredPolicies)

	return filteredPolicies, err
}

// fillSubjectDetail ...
func fillSubjectDetail(r *request.Request) error {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf("Request", "fillSubjectDetail")

	_type := r.Subject.Type
	id := r.Subject.ID

	pk, err := pip.GetSubjectPK(_type, id)
	if err != nil {
		err = errorWrapf(err, "GetSubjectPK _type=`%s`, id=`%s` fail", _type, id)
		return err
	}

	departments, groups, err := pip.GetSubjectDetail(pk)
	if err != nil {
		err = errorWrapf(err, "GetSubjectDetail pk=`%d` fail", pk)
		return err
	}

	r.Subject.FillAttributes(pk, groups, departments)
	return nil
}

// fillActionDetail ...
func fillActionDetail(r *request.Request) error {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf("Request", "fillActionDetail")
	system := r.System
	id := r.Action.ID

	// TODO: to local cache? but, how to notify the changes in /api/query? do nothing currently!
	pk, actionResourceTypes, err := pip.GetActionDetail(system, id)
	if err != nil {
		err = errorWrapf(err, "GetActionDetail system=`%s`, id=`%s` fail", system, id)
		return err
	}

	r.Action.FillAttributes(pk, actionResourceTypes)
	return nil
}
