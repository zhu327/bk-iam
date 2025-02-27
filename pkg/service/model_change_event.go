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
	"iam/pkg/database/dao"
	"iam/pkg/errorx"
	"iam/pkg/service/types"
)

//go:generate mockgen -source=$GOFILE -destination=./mock/$GOFILE -package=mock

const ModelChangeEventSVC = "ModelChangeEventSVC"

// ModelChangeEventService define the interface for model change
type ModelChangeEventService interface {
	ListByStatus(status string) ([]types.ModelChangeEvent, error)
	UpdateStatusByPK(pk int64, status string) error
	BulkCreate(modelChangeEvents []types.ModelChangeEvent) error
	ExistByTypeModel(eventType, status, modelType string, modelPK int64) (bool, error)
}

type modelChangeEventService struct {
	manager dao.ModelChangeEventManager
}

// NewModelChangeService create a ModelChangeEventService
func NewModelChangeService() ModelChangeEventService {
	return &modelChangeEventService{
		manager: dao.NewModelChangeEventManager(),
	}
}

// ListByStatus ...
func (l *modelChangeEventService) ListByStatus(status string) (modelChangeEvents []types.ModelChangeEvent, err error) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf(ModelChangeEventSVC, "ListByStatus")

	dbModelChangeEvents, err := l.manager.ListByStatus(status)
	if err != nil {
		return modelChangeEvents, errorWrapf(err, "ListByStatus(status=%s) fail", status)
	}

	modelChangeEvents = make([]types.ModelChangeEvent, 0, len(dbModelChangeEvents))
	for _, event := range dbModelChangeEvents {
		modelChangeEvents = append(modelChangeEvents, types.ModelChangeEvent{
			PK:        event.PK,
			Type:      event.Type,
			Status:    event.Status,
			SystemID:  event.SystemID,
			ModelType: event.ModelType,
			ModelID:   event.ModelID,
			ModelPK:   event.ModelPK,
		})
	}
	return
}

// UpdateStatusByPK ...
func (l *modelChangeEventService) UpdateStatusByPK(pk int64, status string) (err error) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf(ModelChangeEventSVC, "UpdateStatusByPK")

	err = l.manager.UpdateStatusByPK(pk, status)
	if err != nil {
		return errorWrapf(err, "UpdateStatusByPK(pk=%d, status=%s) fail", pk, status)
	}
	return
}

// BulkCreate ...
func (l *modelChangeEventService) BulkCreate(modelChangeEvents []types.ModelChangeEvent) (err error) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf(ModelChangeEventSVC, "BulkCreate")

	dbModelChangeEvents := make([]dao.ModelChangeEvent, 0, len(modelChangeEvents))
	for _, event := range modelChangeEvents {
		dbModelChangeEvents = append(dbModelChangeEvents, dao.ModelChangeEvent{
			Type:      event.Type,
			Status:    event.Status,
			SystemID:  event.SystemID,
			ModelType: event.ModelType,
			ModelID:   event.ModelID,
			ModelPK:   event.ModelPK,
		})
	}

	err = l.manager.BulkCreate(dbModelChangeEvents)
	if err != nil {
		return errorWrapf(err, "BulkCreate(modelChangeEvents=`%+v`) fail", dbModelChangeEvents)
	}

	return
}

// ExistByTypeModel ...
func (l *modelChangeEventService) ExistByTypeModel(eventType, status, modelType string, modelPK int64) (bool, error) {
	errorWrapf := errorx.NewLayerFunctionErrorWrapf(ModelChangeEventSVC, "ExistByTypeModel")

	event, err := l.manager.GetByTypeModel(eventType, status, modelType, modelPK)
	if err != nil {
		return false, errorWrapf(err, "GetByTypeModel(eventType=%s, modelType=%s, modelPK=%d) fail", eventType,
			modelType, modelPK)
	}

	return event.PK != 0, nil
}
