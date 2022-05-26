/*
 * TencentBlueKing is pleased to support the open source community by making 蓝鲸智云-权限中心(BlueKing-IAM) available.
 * Copyright (C) 2017-2021 THL A29 Limited, a Tencent company. All rights reserved.
 * Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://opensource.org/licenses/MIT
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package handler

import (
	"github.com/TencentBlueKing/gopkg/errorx"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/copier"
	log "github.com/sirupsen/logrus"

	pl "iam/pkg/abac/prp/policy"
	"iam/pkg/api/common"
	"iam/pkg/cacheimpls"
	"iam/pkg/service"
	"iam/pkg/service/types"
	"iam/pkg/util"
)

func batchDeleteMembersFromCache(members []memberSerializer) error {
	pks := make([]int64, 0, len(members))
	for _, m := range members {
		pk, _ := cacheimpls.GetSubjectPK(m.Type, m.ID)
		pks = append(pks, pk)
	}
	return cacheimpls.BatchDeleteSubjectCache(pks)
}

func batchDeleteUpdatedMembersFromCache(members []types.SubjectMember) error {
	pks := make([]int64, 0, len(members))
	for _, m := range members {
		pk, _ := cacheimpls.GetSubjectPK(m.Type, m.ID)
		pks = append(pks, pk)
	}
	return cacheimpls.BatchDeleteSubjectCache(pks)
}

// ListSubject 查询用户/部门/用户组列表
func ListSubject(c *gin.Context) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf("Handler", "ListSubject")

	var body listSubjectSerializer
	if err := c.ShouldBindQuery(&body); err != nil {
		util.BadRequestErrorJSONResponse(c, util.ValidationErrorMessage(err))
		return
	}

	body.Default()

	svc := service.NewSubjectService()
	count, err := svc.GetCount(body.Type)
	if err != nil {
		err = errorWrapf(err, "svc.GetCount type=`%s`", body.Type)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	subjects, err := svc.ListPaging(body.Type, body.Limit, body.Offset)
	if err != nil {
		err = errorWrapf(err, "svc.ListPaging type=`%s` limit=`%d` offset=`%d`",
			body.Type, body.Limit, body.Offset)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	util.SuccessJSONResponse(c, "ok", gin.H{
		"count":   count,
		"results": subjects,
	})
}

// BatchCreateSubjects 批量创建subject
func BatchCreateSubjects(c *gin.Context) {
	var subjects []createSubjectSerializer
	if err := c.ShouldBindJSON(&subjects); err != nil {
		util.BadRequestErrorJSONResponse(c, util.ValidationErrorMessage(err))
		return
	}
	if valid, message := common.ValidateArray(subjects); !valid {
		util.BadRequestErrorJSONResponse(c, message)
		return
	}

	svc := service.NewSubjectService()
	svcSubjects := make([]types.Subject, 0, len(subjects))
	copier.Copy(&svcSubjects, &subjects)

	err := svc.BulkCreate(svcSubjects)
	if err != nil {
		err = errorx.Wrapf(err, "Handler", "BatchCreateSubjects", "subjects=`%+v`", svcSubjects)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	util.SuccessJSONResponse(c, "ok", nil)
}

func deleteGroupPKPolicyCache(groupPKs []int64) {
	// 删除group, 此时group下的所有人subjectDetail 还会有对应的group_pk/dept_pk (这块没有清理, 会导致group虽然被删除,看策略还会被命中)
	// 所以此时需要删除 group 的所有policy cache
	// =>  memory: {system}:{actionPK}:{subjectPK} -> [p1, p2, p3]  | => 这个有change list保证时效
	// =>  redis: {system}:{subjectPK} -> [p1, p2, p3]

	// NOTE: 这里只有group需要delete pks => 其他的呢? 不会有问题, 因为subjectPK被清理了
	// 只delete group policy cache :       groups * system数量 * action数量
	// 不调用这个接口, 删除 group下的所有成员/department下的所有成员的 subjectDetail cache?  groups * 成员列表 * system数量
	var allSystems []types.System
	systemSVC := service.NewSystemService()
	allSystems, err := systemSVC.ListAll()
	if err != nil {
		log.WithError(err).Errorf("deleteGroupPKPolicyCache fail groupPKs=`%v`", groupPKs)
	} else {
		systemIDs := make([]string, 0, len(allSystems))
		for _, s := range allSystems {
			systemIDs = append(systemIDs, s.ID)
		}

		err = pl.BatchDeleteSystemSubjectPKsFromCache(systemIDs, groupPKs)
		if err != nil {
			log.Error(err.Error())
		}
	}
}

// BatchDeleteSubjects 批量删除subject
func BatchDeleteSubjects(c *gin.Context) {
	var subjects []deleteSubjectSerializer
	if err := c.ShouldBindJSON(&subjects); err != nil {
		util.BadRequestErrorJSONResponse(c, util.ValidationErrorMessage(err))
		return
	}
	if valid, message := common.ValidateArray(subjects); !valid {
		util.BadRequestErrorJSONResponse(c, message)
		return
	}
	svc := service.NewSubjectService()
	svcSubjects := make([]types.Subject, 0, len(subjects))
	copier.Copy(&svcSubjects, &subjects)

	// NOTE: collect the type=group subject_pk to delete the cache
	groupPKs := make([]int64, 0, len(subjects))
	for _, s := range svcSubjects {
		if s.Type == types.GroupType {
			gPK, err := cacheimpls.GetSubjectPK(s.Type, s.ID)
			if err != nil {
				log.WithError(err).Errorf("BatchDeleteSubjects getSubjectPK fail type=`%s`, id=`%s`", s.Type, s.ID)
				continue
			}
			groupPKs = append(groupPKs, gPK)
		}
	}

	pks, err := svc.BulkDelete(svcSubjects)
	if err != nil {
		err = errorx.Wrapf(err, "Handler", "BatchDeleteSubjects",
			"svc.BulkDelete subjects=`%v`", svcSubjects)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	// 清除涉及的所有缓存 [subjectGroup / subjectDetails]
	cacheimpls.BatchDeleteSubjectCache(pks)

	for _, s := range subjects {
		cacheimpls.DeleteSubjectPK(s.Type, s.ID)
		cacheimpls.DeleteLocalSubjectPK(s.Type, s.ID)
	}
	// Note: 不需要清除subject的成员其对应的SubjectGroup和SubjectDepartment，
	//       =>  保证拿到的group pk 没有对应的policy cache/回源也查不到
	deleteGroupPKPolicyCache(groupPKs)

	util.SuccessJSONResponse(c, "ok", nil)
}

// ListSubjectMember 查询用户组的成员列表
func ListSubjectMember(c *gin.Context) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf("Handler", "ListSubjectMember")

	var subject listSubjectMemberSerializer
	if err := c.ShouldBindQuery(&subject); err != nil {
		util.BadRequestErrorJSONResponse(c, util.ValidationErrorMessage(err))
		return
	}

	subject.Default()

	svc := service.NewSubjectService()
	count, err := svc.GetMemberCount(subject.Type, subject.ID)
	if err != nil {
		err = errorWrapf(err, "type=`%s`, id=`%s`", subject.Type, subject.ID)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	relations, err := svc.ListPagingMember(subject.Type, subject.ID, subject.Limit, subject.Offset)
	if err != nil {
		err = errorWrapf(err, "type=`%s`, id=`%s`, limit=`%d`, offset=`%d`",
			subject.Type, subject.ID, subject.Limit, subject.Offset)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	util.SuccessJSONResponse(c, "ok", gin.H{
		"count":   count,
		"results": relations,
	})
}

// GetSubjectGroup 获取subject关联的用户组
func GetSubjectGroup(c *gin.Context) {
	var subject subjectRelationSerializer
	if err := c.ShouldBindQuery(&subject); err != nil {
		util.BadRequestErrorJSONResponse(c, util.ValidationErrorMessage(err))
		return
	}

	svc := service.NewSubjectService()
	groups, err := svc.ListSubjectGroups(subject.Type, subject.ID, subject.BeforeExpiredAt)
	if err != nil {
		err = errorx.Wrapf(err, "Handler", "ListSubjectGroups",
			"type=`%s`, id=`%s`", subject.Type, subject.ID)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	util.SuccessJSONResponse(c, "ok", groups)
}

// UpdateSubjectMembersExpiredAt subject关系续期
func UpdateSubjectMembersExpiredAt(c *gin.Context) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf("Handler", "UpdateSubjectMembersExpiredAt")

	var body subjectMemberExpiredAtSerializer
	if err := c.ShouldBindJSON(&body); err != nil {
		util.BadRequestErrorJSONResponse(c, util.ValidationErrorMessage(err))
		return
	}

	if ok, message := body.validate(); !ok {
		util.BadRequestErrorJSONResponse(c, message)
		return
	}

	subjects := make([]types.SubjectWithExpiredAt, 0, len(body.Members))
	for _, m := range body.Members {
		subjects = append(subjects, types.SubjectWithExpiredAt{
			Type:            m.Type,
			ID:              m.ID,
			PolicyExpiredAt: m.PolicyExpiredAt,
		})
	}

	svc := service.NewSubjectService()
	err := svc.UpdateMembersExpiredAt(body.Type, body.ID, subjects)
	if err != nil {
		err = errorWrapf(
			err, "svc.UpdateMembersExpiredAt type=`%s` id=`%s` subjects=`%+v`",
			body.Type, body.ID, subjects,
		)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	// TODO 处理缓存清理

	// 清除涉及用户的缓存
	// batchDeleteUpdatedMembersFromCache(updateMembers)

	util.SuccessJSONResponse(c, "ok", gin.H{})
}

// DeleteSubjectMembers 批量删除subject成员
func DeleteSubjectMembers(c *gin.Context) {
	var body deleteSubjectMemberSerializer
	if err := c.ShouldBindJSON(&body); err != nil {
		util.BadRequestErrorJSONResponse(c, util.ValidationErrorMessage(err))
		return
	}
	if valid, message := common.ValidateArray(body.Members); !valid {
		util.BadRequestErrorJSONResponse(c, message)
		return
	}

	svcSubjects := make([]types.Subject, 0, len(body.Members))
	copier.Copy(&svcSubjects, &body.Members)

	svc := service.NewSubjectService()
	typeCount, err := svc.BulkDeleteSubjectMembers(body.Type, body.ID, svcSubjects)
	if err != nil {
		err = errorx.Wrapf(err, "Handler", "DeleteSubjectMembers",
			"type=`%s`, id=`%s`, subjects=`%+v`", body.Type, body.ID, svcSubjects)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	batchDeleteMembersFromCache(body.Members)
	// TODO: 这里可以区分 dept -> group关系变更

	util.SuccessJSONResponse(c, "ok", typeCount)
}

// BatchAddSubjectMembers 批量添加subject成员
func BatchAddSubjectMembers(c *gin.Context) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf("Handler", "BatchAddSubjectMembers")

	var body addSubjectMembersSerializer
	if err := c.ShouldBindJSON(&body); err != nil {
		util.BadRequestErrorJSONResponse(c, util.ValidationErrorMessage(err))
		return
	}
	if ok, message := body.validate(); !ok {
		util.BadRequestErrorJSONResponse(c, message)
		return
	}

	subjects := make([]types.SubjectWithExpiredAt, 0, len(body.Members))
	for _, m := range body.Members {
		subjects = append(subjects, types.SubjectWithExpiredAt{
			Type:            m.Type,
			ID:              m.ID,
			PolicyExpiredAt: body.PolicyExpiredAt,
		})
	}

	svc := service.NewSubjectService()
	typeCount, err := svc.BulkCreateSubjectMembers(body.Type, body.ID, subjects)
	if err != nil {
		err = errorWrapf(
			err, "svc.UpdateMembersExpiredAt type=`%s` id=`%s` subjects=`%+v`",
			body.Type, body.ID, subjects,
		)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	// TODO 处理缓存清理

	// 清除 更新了过期时间的成员的cache
	// batchDeleteUpdatedMembersFromCache(updateMembers)

	// 清除涉及用户的缓存
	// batchDeleteMembersFromCache(body.Members)
	// TODO: 这里可以区分 dept -> group关系变更
	util.SuccessJSONResponse(c, "ok", typeCount)
}

// BatchCreateSubjectDepartments ...
func BatchCreateSubjectDepartments(c *gin.Context) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf("Handler", "BatchCreateSubjectDepartments")

	var subjectDepartments []subjectDepartment
	if err := c.ShouldBindJSON(&subjectDepartments); err != nil {
		util.BadRequestErrorJSONResponse(c, util.ValidationErrorMessage(err))
		return
	}

	svcSubjectDepartments := make([]types.SubjectDepartment, 0, len(subjectDepartments))
	for _, sd := range subjectDepartments {
		svcSubjectDepartments = append(svcSubjectDepartments, types.SubjectDepartment{
			SubjectID:     sd.SubjectID,
			DepartmentIDs: sd.DepartmentIDs,
		})
	}

	svc := service.NewSubjectService()
	err := svc.BulkCreateSubjectDepartments(svcSubjectDepartments)
	if err != nil {
		err = errorWrapf(err, "svc.BulkCreateSubjectDepartments subjectDepartments=`%+v`", svcSubjectDepartments)
		util.SystemErrorJSONResponse(c, err)
		return
	}
	// TODO: 确认这里没有清subject detail?

	util.SuccessJSONResponse(c, "ok", nil)
}

// BatchDeleteSubjectDepartments ...
func BatchDeleteSubjectDepartments(c *gin.Context) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf("Handler", "BatchDeleteSubjectDepartments")

	var subjectIDs []string
	if err := c.ShouldBindJSON(&subjectIDs); err != nil {
		util.BadRequestErrorJSONResponse(c, util.ValidationErrorMessage(err))
		return
	}

	if len(subjectIDs) == 0 {
		util.BadRequestErrorJSONResponse(c, "subject id can not be empty")
		return
	}

	svc := service.NewSubjectService()
	pks, err := svc.BulkDeleteSubjectDepartments(subjectIDs)
	if err != nil {
		err = errorWrapf(err, "svc.BulkUpdateSubjectDepartments BulkDeleteSubjectDepartments=`%+v`", subjectIDs)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	// delete from cache
	cacheimpls.BatchDeleteSubjectCache(pks)

	util.SuccessJSONResponse(c, "ok", nil)
}

// BatchUpdateSubjectDepartments ...
func BatchUpdateSubjectDepartments(c *gin.Context) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf("Handler", "BatchUpdateSubjectDepartments")

	var subjectDepartments []subjectDepartment
	if err := c.ShouldBindJSON(&subjectDepartments); err != nil {
		util.BadRequestErrorJSONResponse(c, util.ValidationErrorMessage(err))
		return
	}

	svcSubjectDepartments := make([]types.SubjectDepartment, 0, len(subjectDepartments))
	for _, sd := range subjectDepartments {
		svcSubjectDepartments = append(svcSubjectDepartments, types.SubjectDepartment{
			SubjectID:     sd.SubjectID,
			DepartmentIDs: sd.DepartmentIDs,
		})
	}

	svc := service.NewSubjectService()
	pks, err := svc.BulkUpdateSubjectDepartments(svcSubjectDepartments)
	if err != nil {
		err = errorWrapf(err, "svc.BulkUpdateSubjectDepartments subjectDepartments=`%+v`", svcSubjectDepartments)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	// delete from cache
	cacheimpls.BatchDeleteSubjectCache(pks)

	util.SuccessJSONResponse(c, "ok", nil)
}

// ListSubjectDepartments ...
func ListSubjectDepartments(c *gin.Context) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf("Handler", "ListSubjectDepartments")

	var page pageSerializer
	if err := c.ShouldBindQuery(&page); err != nil {
		util.BadRequestErrorJSONResponse(c, util.ValidationErrorMessage(err))
		return
	}

	page.Default()

	svc := service.NewSubjectService()
	count, err := svc.GetSubjectDepartmentCount()
	if err != nil {
		util.SystemErrorJSONResponse(c, errorWrapf(err, "svc.GetSubjectDepartmentCount"))
		return
	}

	subjectDepartments, err := svc.ListPagingSubjectDepartment(page.Limit, page.Offset)
	if err != nil {
		err = errorWrapf(err, "svc.ListPagingSubjectDepartment limit=`%d`, offset=`%d`", page.Limit, page.Offset)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	util.SuccessJSONResponse(c, "ok", gin.H{
		"count":   count,
		"results": subjectDepartments,
	})
}

// BatchUpdateSubject ...
func BatchUpdateSubject(c *gin.Context) {
	var subjects []updateSubjectSerializer
	if err := c.ShouldBindJSON(&subjects); err != nil {
		util.BadRequestErrorJSONResponse(c, util.ValidationErrorMessage(err))
		return
	}
	if valid, message := common.ValidateArray(subjects); !valid {
		util.BadRequestErrorJSONResponse(c, message)
		return
	}

	svcSubjects := make([]types.Subject, 0, len(subjects))
	copier.Copy(&svcSubjects, &subjects)

	svc := service.NewSubjectService()
	err := svc.BulkUpdateName(svcSubjects)

	errorWrapf := errorx.NewLayerFunctionErrorWrapf("Handler", "BatchUpdateSubject")
	if err != nil {
		err = errorWrapf(err, "svc.BulkUpdateName subjects=`%+v`", svcSubjects)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	util.SuccessJSONResponse(c, "ok", nil)
}

// CreateSubjectRole ...
func CreateSubjectRole(c *gin.Context) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf("Handler", "BulkCreateSubjectRole")

	var body subjectRoleSerializer
	if err := c.ShouldBindJSON(&body); err != nil {
		util.BadRequestErrorJSONResponse(c, util.ValidationErrorMessage(err))
		return
	}
	if ok, message := body.validate(); !ok {
		util.BadRequestErrorJSONResponse(c, message)
		return
	}

	svcSubjects := make([]types.Subject, 0, len(body.Subjects))
	copier.Copy(&svcSubjects, &body.Subjects)

	// TODO: 校验 systemID 存在

	svc := service.NewSubjectService()
	err := svc.BulkCreateSubjectRoles(body.RoleType, body.SystemID, svcSubjects)

	if err != nil {
		err = errorWrapf(
			err,
			"svc.BulkCreateSubjectRoles roleType=`%s`, system=`%s`, subjects=`%+v`",
			body.RoleType,
			body.SystemID,
			svcSubjects,
		)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	// clean cache
	for _, subject := range svcSubjects {
		cacheimpls.DeleteSubjectRoleSystemID(subject.Type, subject.ID)
	}

	util.SuccessJSONResponse(c, "ok", nil)
}

// DeleteSubjectRole ...
func DeleteSubjectRole(c *gin.Context) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf("Handler", "BulkDeleteSubjectRole")

	var body subjectRoleSerializer
	if err := c.ShouldBindJSON(&body); err != nil {
		util.BadRequestErrorJSONResponse(c, util.ValidationErrorMessage(err))
		return
	}
	if ok, message := body.validate(); !ok {
		util.BadRequestErrorJSONResponse(c, message)
		return
	}

	svcSubjects := make([]types.Subject, 0, len(body.Subjects))
	copier.Copy(&svcSubjects, &body.Subjects)

	svc := service.NewSubjectService()
	err := svc.BulkDeleteSubjectRoles(body.RoleType, body.SystemID, svcSubjects)

	if err != nil {
		err = errorWrapf(
			err,
			"svc.BulkDeleteSubjectRoles roleType=`%s`, system=`%s`, subjects=`%+v`",
			body.RoleType,
			body.SystemID,
			svcSubjects,
		)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	// clean cache
	for _, subject := range svcSubjects {
		cacheimpls.DeleteSubjectRoleSystemID(subject.Type, subject.ID)
	}

	util.SuccessJSONResponse(c, "ok", nil)
}

// ListSubjectRole ...
func ListSubjectRole(c *gin.Context) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf("Handler", "BulkDeleteSubjectRole")

	var query subjectRoleQuerySerializer

	if err := c.ShouldBindQuery(&query); err != nil {
		util.BadRequestErrorJSONResponse(c, util.ValidationErrorMessage(err))
		return
	}

	if valid, message := query.validate(); !valid {
		util.BadRequestErrorJSONResponse(c, message)
		return
	}

	svc := service.NewSubjectService()
	subjectPKs, err := svc.ListSubjectPKByRole(query.RoleType, query.SystemID)
	if err != nil {
		err = errorWrapf(
			err,
			"svc.ListSubjectPKByRole roleType=`%s`, system=`%s`",
			query.RoleType,
			query.SystemID,
		)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	subjects, err := svc.ListByPKs(subjectPKs)
	if err != nil {
		err = errorWrapf(err, "svc.ListByPKs pks=`%+v`", subjectPKs)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	util.SuccessJSONResponse(c, "ok", subjects)
}

// ListSubjectMemberBeforeExpiredAt 获取小于指定过期时间的成员列表
func ListSubjectMemberBeforeExpiredAt(c *gin.Context) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf("Handler", "ListSubjectmembersBeforeExpiredAt")

	var body listSubjectMemberBeforeExpiredAtSerializer
	if err := c.ShouldBindQuery(&body); err != nil {
		util.BadRequestErrorJSONResponse(c, util.ValidationErrorMessage(err))
		return
	}

	body.Default()

	svc := service.NewSubjectService()
	count, err := svc.GetMemberCountBeforeExpiredAt(body.Type, body.ID, body.BeforeExpiredAt)
	if err != nil {
		err = errorWrapf(err, "type=`%s`, id=`%s`, beforeExpiredAt=`%d`", body.Type, body.ID, body.BeforeExpiredAt)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	relations, err := svc.ListPagingMemberBeforeExpiredAt(
		body.Type, body.ID, body.BeforeExpiredAt, body.Limit, body.Offset,
	)
	if err != nil {
		err = errorWrapf(err, "type=`%s`, id=`%s`, beforeExpiredAt=`%d`", body.Type, body.ID, body.BeforeExpiredAt)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	util.SuccessJSONResponse(c, "ok", gin.H{
		"count":   count,
		"results": relations,
	})
}

// ListExistSubjectsBeforeExpiredAt 筛选出有成员过期的subjects
func ListExistSubjectsBeforeExpiredAt(c *gin.Context) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf("Handler", "FilterSubjectsBeforeExpiredAt")

	var body filterSubjectsBeforeExpiredAtSerializer
	if err := c.ShouldBindJSON(&body); err != nil {
		util.BadRequestErrorJSONResponse(c, util.ValidationErrorMessage(err))
		return
	}
	if ok, message := body.validate(); !ok {
		util.BadRequestErrorJSONResponse(c, message)
		return
	}

	svcSubjects := make([]types.Subject, 0, len(body.Subjects))
	copier.Copy(&svcSubjects, &body.Subjects)

	svc := service.NewSubjectService()
	existSubjects, err := svc.ListExistSubjectsBeforeExpiredAt(svcSubjects, body.BeforeExpiredAt)
	if err != nil {
		err = errorWrapf(err, "subjects=`%+v`, beforeExpiredAt=`%d`", svcSubjects, body.BeforeExpiredAt)
		util.SystemErrorJSONResponse(c, err)
		return
	}

	util.SuccessJSONResponse(c, "ok", existSubjects)
}
