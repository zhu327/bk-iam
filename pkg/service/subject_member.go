/*
 * TencentBlueKing is pleased to support the open source community by making 蓝鲸智云-权限中心(BlueKing-IAM) available.
 * Copyright (C) 2017-2021 THL A29 Limited, a Tencent company. All rights reserved.
 * Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://opensource.org/licenses/MIT
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package service

import (
	"fmt"

	"github.com/TencentBlueKing/gopkg/errorx"

	"iam/pkg/database"
	"iam/pkg/database/dao"
	"iam/pkg/service/types"
)

// GetMemberCount ...
func (l *subjectService) GetMemberCount(_type, id string) (int64, error) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf(SubjectSVC, "GetMemberCount")
	// TODO 后续通过缓存提高性能
	pk, err := l.manager.GetPK(_type, id)
	if err != nil {
		return 0, errorWrapf(err, "manager.GetPK _type=`%s`, id=`%s` fail", _type, id)
	}

	count, err := l.relationManager.GetMemberCount(pk)
	if err != nil {
		err = errorWrapf(err, "relationManager.GetMemberCount _type=`%s`, id=`%s` fail", _type, id)
		return 0, err
	}
	return count, nil
}

// ListPagingMember ...
func (l *subjectService) ListPagingMember(_type, id string, limit, offset int64) ([]types.SubjectMember, error) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf(SubjectSVC, "ListPagingMember")
	// 查询subject PK
	pk, err := l.manager.GetPK(_type, id)
	if err != nil {
		return nil, errorWrapf(err, "manager.GetPK _type=`%s`, id=`%s` fail", _type, id)
	}

	daoRelations, err := l.relationManager.ListPagingMember(pk, limit, offset)
	if err != nil {
		return nil, errorWrapf(err, "relationManager.ListPagingMember _type=`%s`, id=`%s`, limit=`%d`, offset=`%d`",
			_type, id, limit, offset)
	}

	members, err := l.convertToSubjectMembers(daoRelations)
	if err != nil {
		return nil, errorWrapf(err, "convertToSubjectMembers relations=`%s`", members)
	}

	return members, nil
}

func (l *subjectService) getSubjectMapByPKs(pks []int64) (map[int64]dao.Subject, error) {
	if len(pks) == 0 {
		return nil, nil
	}

	subjects, err := l.manager.ListByPKs(pks)
	if err != nil {
		return nil, err
	}

	subjectMap := make(map[int64]dao.Subject, len(subjects))
	for _, s := range subjects {
		subjectMap[s.PK] = s
	}
	return subjectMap, nil
}

func (l *subjectService) convertToSubjectMembers(daoRelations []dao.SubjectRelation) ([]types.SubjectMember, error) {
	if len(daoRelations) == 0 {
		return nil, nil
	}

	subjectPKs := make([]int64, 0, len(daoRelations))
	for _, r := range daoRelations {
		subjectPKs = append(subjectPKs, r.SubjectPK)
	}

	// TODO 后续通过缓存提高性能
	subjectMap, err := l.getSubjectMapByPKs(subjectPKs)
	if err != nil {
		return nil, err
	}

	members := make([]types.SubjectMember, 0, len(daoRelations))
	for _, r := range daoRelations {
		var _type, id string
		subject, ok := subjectMap[r.SubjectPK]
		if ok {
			_type = subject.Type
			id = subject.ID
		}

		members = append(members, types.SubjectMember{
			PK:              r.PK,
			Type:            _type,
			ID:              id,
			PolicyExpiredAt: r.PolicyExpiredAt,
			CreateAt:        r.CreateAt,
		})
	}
	return members, nil
}

// ListMember ...
func (l *subjectService) ListMember(_type, id string) ([]types.SubjectMember, error) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf(SubjectSVC, "ListMember")
	// 查询subject PK
	pk, err := l.manager.GetPK(_type, id)
	if err != nil {
		return nil, errorWrapf(err, "manager.GetPK _type=`%s`, id=`%s` fail", _type, id)
	}

	daoRelations, err := l.relationManager.ListMember(pk)
	if err != nil {
		return nil, errorx.Wrapf(err, SubjectSVC,
			"ListMember", "relationManager.ListMember _type=`%s`, id=`%s` fail", _type, id)
	}

	members, err := l.convertToSubjectMembers(daoRelations)
	if err != nil {
		return nil, errorWrapf(err, "convertToSubjectMembers relations=`%s`", members)
	}

	return members, nil
}

// UpdateMembersExpiredAt ...
func (l *subjectService) UpdateMembersExpiredAt(_type, id string, subjectWithExpiredAts []types.SubjectWithExpiredAt) error {
	_, err := l.bulkCreateOrUpdateSubjectMembers(_type, id, subjectWithExpiredAts, false)
	return err
}

// BulkDeleteSubjectMembers ...
func (l *subjectService) BulkDeleteSubjectMembers(_type, id string, members []types.Subject) (map[string]int64, error) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf(SubjectSVC, "BulkDeleteSubjectMember")

	// 查询subject PK
	parentPK, err := l.manager.GetPK(_type, id)
	if err != nil {
		return nil, errorWrapf(err, "manager.GetPK _type=`%s`, id=`%s` fail", _type, id)
	}

	// 使用事务
	tx, err := database.GenerateDefaultDBTx()
	defer database.RollBackWithLog(tx)

	if err != nil {
		return nil, errorWrapf(err, "define tx error")
	}

	// 查询dao subject
	userPKs, departmentPKs, err := l.splitSubjectToPK(members)
	if err != nil {
		return nil, errorWrapf(err, "splitSubjectToPK subjects=`%+v` fail", members)
	}

	typeCount := map[string]int64{
		types.UserType:       0,
		types.DepartmentType: 0,
	}

	// 处理用户的删除
	if len(userPKs) != 0 {
		count, err := l.relationManager.BulkDeleteByMembersWithTx(tx, parentPK, userPKs)
		if err != nil {
			return nil, errorWrapf(
				err, "relationManager.BulkDeleteByMembersWithTx parentPK=`%s`, userPKs=`%+v` fail",
				parentPK, userPKs,
			)
		}

		typeCount[types.UserType] = count
	}

	// 处理部门的删除
	if len(departmentPKs) != 0 {
		count, err := l.relationManager.BulkDeleteByMembersWithTx(tx, parentPK, departmentPKs)
		if err != nil {
			return nil, errorWrapf(
				err, "relationManager.BulkDeleteByMembersWithTx parentPK=`%s`, departmentPKs=`%+v` fail",
				parentPK, departmentPKs,
			)
		}

		typeCount[types.DepartmentType] = count
	}

	subjectPKs := append(userPKs, departmentPKs...)
	// 更新subject_system_groups表的groups字段
	err = l.bulkDeleteSubjectSystemGroup(tx, parentPK, subjectPKs)
	if err != nil {
		return nil, errorWrapf(
			err, "bulkDeleteSubjectSystemGroup parentPK=`%d`, subjectPKs=`%+v` fail",
			parentPK, subjectPKs,
		)
	}

	err = tx.Commit()
	if err != nil {
		return nil, errorWrapf(err, "tx commit error")
	}
	return typeCount, err
}

// BulkCreateSubjectMembers ...
func (l *subjectService) BulkCreateSubjectMembers(
	_type, id string,
	subjectWithExpiredAts []types.SubjectWithExpiredAt,
) (map[string]int64, error) {
	return l.bulkCreateOrUpdateSubjectMembers(_type, id, subjectWithExpiredAts, true)
}

// bulkCreateOrUpdateSubjectMembers ...
func (l *subjectService) bulkCreateOrUpdateSubjectMembers(
	_type, id string,
	subjectWithExpiredAts []types.SubjectWithExpiredAt,
	createIfNotExists bool,
) (map[string]int64, error) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf(SubjectSVC, "bulkCreateOrUpdateSubjectMembers")

	// 查询subject PK
	parentPK, err := l.manager.GetPK(_type, id)
	if err != nil {
		return nil, errorWrapf(err, "manager.GetPK _type=`%s`, id=`%s` fail", _type, id)
	}

	// 查询group已有的成员
	daoRelationMap, err := l.getDaoRelationMap(parentPK)
	if err != nil {
		return nil, errorWrapf(err, "getDaoRelationMap parentPK=`%d` fail", parentPK)
	}

	// 查询subject
	daoSubjects, err := l.listDaoSubject(subjectWithExpiredAts)
	if err != nil {
		return nil, errorWrapf(err, "newMethod subjectWithExpiredAts=`%s` fail", subjectWithExpiredAts)
	}

	subjectExpiredAtMap := genSubjectExpiredAtMap(subjectWithExpiredAts)

	// 用于更新subject relation
	updateRelations := make([]dao.SubjectRelationPKPolicyExpiredAt, 0, len(subjectWithExpiredAts))

	// 用于创建的subject relation
	createRelations := make([]dao.SubjectRelation, 0, len(subjectWithExpiredAts))

	// 用于更新subject system group
	subjectPKWithExpiredAts := make([]types.SubjectPKWithExpiredAt, 0, len(subjectWithExpiredAts))

	// 创建的成员数量
	typeCount := map[string]int64{
		types.UserType:       0,
		types.DepartmentType: 0,
	}

	// 生成需要更新的数据
	for _, s := range daoSubjects {
		key := fmt.Sprintf("%s:%s", s.Type, s.ID)

		daoRelation, ok := daoRelationMap[s.PK]
		if ok && daoRelation.PolicyExpiredAt < subjectExpiredAtMap[key] {
			updateRelations = append(updateRelations, dao.SubjectRelationPKPolicyExpiredAt{
				PK:              daoRelation.PK,
				PolicyExpiredAt: subjectExpiredAtMap[key],
			})

			subjectPKWithExpiredAts = append(subjectPKWithExpiredAts, types.SubjectPKWithExpiredAt{
				SubjectPK: s.PK,
				ExpiredAt: subjectExpiredAtMap[key],
			})
		} else if createIfNotExists && !ok {
			createRelations = append(createRelations, dao.SubjectRelation{
				SubjectPK:       s.PK,
				ParentPK:        parentPK,
				PolicyExpiredAt: subjectExpiredAtMap[key],
			})

			subjectPKWithExpiredAts = append(subjectPKWithExpiredAts, types.SubjectPKWithExpiredAt{
				SubjectPK: s.PK,
				ExpiredAt: subjectExpiredAtMap[key],
			})

			switch s.Type {
			case types.UserType:
				typeCount[types.UserType]++
			case types.DepartmentType:
				typeCount[types.DepartmentType]++
			}
		}
	}

	// 使用事务
	tx, err := database.GenerateDefaultDBTx()
	defer database.RollBackWithLog(tx)

	if err != nil {
		return nil, errorWrapf(err, "define tx error")
	}

	// 更新subject relation
	err = l.relationManager.UpdateExpiredAtWithTx(tx, updateRelations)
	if err != nil {
		return nil, errorWrapf(err,
			"relationManager.UpdateExpiredAt relations=`%+v` fail", updateRelations)
	}

	if createIfNotExists && len(createRelations) != 0 {
		// 创建subject relation
		err = l.relationManager.BulkCreateWithTx(tx, createRelations)
		if err != nil {
			return nil, errorWrapf(err, "relationManager.BulkCreateWithTx relations=`%+v` fail", createRelations)
		}
	}

	// 更新subject system group
	err = l.bulkUpdateSubjectSystemGroup(tx, parentPK, subjectPKWithExpiredAts)
	if err != nil {
		return nil, errorWrapf(
			err, "bulkUpdateSubjectSystemGroup parentPK=`%d`, subjectPKWithExpiredAts=`%+v` fail",
			parentPK, subjectPKWithExpiredAts,
		)
	}

	return typeCount, nil
}

func genSubjectExpiredAtMap(subjectWithExpiredAts []types.SubjectWithExpiredAt) map[string]int64 {
	subjectExpiredAt := make(map[string]int64, len(subjectWithExpiredAts))
	for _, s := range subjectWithExpiredAts {
		subjectExpiredAt[fmt.Sprintf("%s:%s", s.Type, s.ID)] = s.PolicyExpiredAt
	}
	return subjectExpiredAt
}

func (l *subjectService) listDaoSubject(subjectWithExpiredAts []types.SubjectWithExpiredAt) ([]dao.Subject, error) {
	subjects := make([]types.Subject, 0, len(subjectWithExpiredAts))
	for _, s := range subjectWithExpiredAts {
		subjects = append(subjects, types.Subject{
			Type: s.Type,
			ID:   s.ID,
		})
	}

	users, departments, err := l.splitSubject(subjects)
	if err != nil {
		return nil, err
	}
	return append(users, departments...), nil
}

func (l *subjectService) getDaoRelationMap(parentPK int64) (map[int64]dao.SubjectRelation, error) {
	daoRelations, err := l.relationManager.ListMember(parentPK)
	if err != nil {
		return nil, err
	}

	daoRelationMap := make(map[int64]dao.SubjectRelation, len(daoRelations))
	for _, r := range daoRelations {
		daoRelationMap[r.SubjectPK] = r
	}
	return daoRelationMap, nil
}

// splitSubject 分离subject to userPKs departmentPKs
func (l *subjectService) splitSubject(
	subjects []types.Subject,
) (users []dao.Subject, departments []dao.Subject, err error) {
	if len(subjects) == 0 {
		return nil, nil, nil
	}

	// 按类型分组
	userIDs, departmentIDs, _ := groupBySubjectType(subjects)

	// 查询user PK
	users, err = l.manager.ListByIDs(types.UserType, userIDs)
	if err != nil {
		return nil, nil, err
	}

	// 查询department PK
	departments, err = l.manager.ListByIDs(types.DepartmentType, departmentIDs)
	if err != nil {
		return nil, nil, err
	}

	return
}

func (l *subjectService) splitSubjectToPK(
	subjects []types.Subject,
) (userPKs []int64, departmentPKs []int64, err error) {
	if len(subjects) == 0 {
		return nil, nil, nil
	}

	users, departments, err := l.splitSubject(subjects)
	if err != nil {
		return nil, nil, err
	}

	userPKs = make([]int64, 0, len(users))
	for _, u := range users {
		userPKs = append(userPKs, u.PK)
	}

	departmentPKs = make([]int64, 0, len(departments))
	for _, d := range departments {
		departmentPKs = append(departmentPKs, d.PK)
	}

	return
}

// GetMemberCountBeforeExpiredAt ...
func (l *subjectService) GetMemberCountBeforeExpiredAt(_type, id string, expiredAt int64) (int64, error) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf(SubjectSVC, "GetMemberCountBeforeExpiredAt")
	// 查询subject PK
	parentPK, err := l.manager.GetPK(_type, id)
	if err != nil {
		return 0, errorWrapf(err, "manager.GetPK _type=`%s`, id=`%s` fail", _type, id)
	}

	count, err := l.relationManager.GetMemberCountBeforeExpiredAt(parentPK, expiredAt)
	if err != nil {
		err = errorx.Wrapf(err, SubjectSVC, "GetMemberCountBeforeExpiredAt",
			"relationManager.GetMemberCountBeforeExpiredAt _type=`%s`, id=`%s`, expiredAt=`%d` fail",
			_type, id, expiredAt)
		return 0, err
	}
	return count, nil
}

// ListPagingMemberBeforeExpiredAt ...
func (l *subjectService) ListPagingMemberBeforeExpiredAt(
	_type, id string, expiredAt int64, limit, offset int64,
) ([]types.SubjectMember, error) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf(SubjectSVC, "ListPagingMemberBeforeExpiredAt")
	// 查询subject PK
	parentPK, err := l.manager.GetPK(_type, id)
	if err != nil {
		return nil, errorWrapf(err, "manager.GetPK _type=`%s`, id=`%s` fail", _type, id)
	}

	daoRelations, err := l.relationManager.ListPagingMemberBeforeExpiredAt(
		parentPK, expiredAt, limit, offset)
	if err != nil {
		return nil, errorx.Wrapf(err, SubjectSVC,
			"ListPagingMemberBeforeExpiredAt", "_type=`%s`, id=`%s`, expiredAt=`%d`, limit=`%d`, offset=`%d`",
			_type, id, expiredAt, limit, offset)
	}
	members, err := l.convertToSubjectMembers(daoRelations)
	if err != nil {
		return nil, errorWrapf(err, "convertToSubjectMembers relations=`%s`", members)
	}

	return members, nil
}
