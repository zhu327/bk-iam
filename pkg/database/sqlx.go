/*
 * TencentBlueKing is pleased to support the open source community by making 蓝鲸智云-权限中心(BlueKing-IAM) available.
 * Copyright (C) 2017-2021 THL A29 Limited, a Tencent company. All rights reserved.
 * Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://opensource.org/licenses/MIT
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package database

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"

	"iam/pkg/util"
)

// ============== timer ==============
type queryFunc func(db *sqlx.DB, dest interface{}, query string, args ...interface{}) error

func queryTimer(f queryFunc) queryFunc {
	return func(db *sqlx.DB, dest interface{}, query string, args ...interface{}) error {
		start := time.Now()
		defer logSlowSQL(start, query, args)
		// NOTE: must be args...
		return f(db, dest, query, args...)
	}
}

type deleteFunc func(db *sqlx.DB, query string, args ...interface{}) (int64, error)

func deleteTimer(f deleteFunc) deleteFunc {
	return func(db *sqlx.DB, query string, args ...interface{}) (int64, error) {
		start := time.Now()
		defer logSlowSQL(start, query, args)
		// NOTE: must be args...
		return f(db, query, args...)
	}
}

type deleteWithCtxFunc func(ctx context.Context, db *sqlx.DB, query string, args ...interface{}) (int64, error)

func deleteWithCtxTimer(f deleteWithCtxFunc) deleteWithCtxFunc {
	return func(ctx context.Context, db *sqlx.DB, query string, args ...interface{}) (int64, error) {
		start := time.Now()
		defer logSlowSQL(start, query, args)
		// NOTE: must be args...
		return f(ctx, db, query, args...)
	}
}

type updateFunc func(db *sqlx.DB, query string, args interface{}) (int64, error)

func updateTimer(f updateFunc) updateFunc {
	return func(db *sqlx.DB, query string, args interface{}) (int64, error) {
		start := time.Now()
		defer logSlowSQL(start, query, args)
		return f(db, query, args)
	}
}

type bulkInsertFunc func(db *sqlx.DB, query string, args interface{}) error

func bulkInsertTimer(f bulkInsertFunc) bulkInsertFunc {
	return func(db *sqlx.DB, query string, args interface{}) error {
		start := time.Now()
		defer logSlowSQL(start, query, args)
		return f(db, query, args)
	}
}

// ================== raw execute func ==================
func sqlxSelectFunc(db *sqlx.DB, dest interface{}, query string, args ...interface{}) error {
	query, args, err := sqlx.In(query, args...)
	if err != nil {
		return err
	}
	err = db.Select(dest, query, args...)
	return err
}

func sqlxGetFunc(db *sqlx.DB, dest interface{}, query string, args ...interface{}) error {
	query, args, err := sqlx.In(query, args...)
	if err != nil {
		return err
	}
	err = db.Get(dest, query, args...)

	if err == nil {
		return nil
	}

	return err
}

func sqlxDeleteFunc(db *sqlx.DB, query string, args ...interface{}) (int64, error) {
	query, args, err := sqlx.In(query, args...)
	if err != nil {
		return 0, err
	}

	result, err := db.Exec(query, args...)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()

	if err != nil {
		return 0, err
	}

	return rowsAffected, nil
}

func sqlxDeleteWithCtxFunc(ctx context.Context, db *sqlx.DB, query string, args ...interface{}) (int64, error) {
	query, args, err := sqlx.In(query, args...)
	if err != nil {
		return 0, err
	}

	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()

	if err != nil {
		return 0, err
	}

	return rowsAffected, nil
}

func sqlxUpdateFunc(db *sqlx.DB, query string, args interface{}) (int64, error) {
	result, err := db.NamedExec(query, args)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return rowsAffected, nil
}

func sqlxBulkInsertFunc(db *sqlx.DB, query string, args interface{}) error {
	q, arrayArgs, err := bindArray(sqlx.BindType(db.DriverName()), query, args, db.Mapper)
	if err != nil {
		return err
	}
	_, err = db.Exec(q, arrayArgs...)
	return err
}

// NOTE 重BulkInsert复制, BulkInsert可能会修改, 注意不要复用
func sqlxBulkUpdateFunc(db *sqlx.DB, query string, args interface{}) error {
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer RollBackWithLog(tx)

	err = SqlxBulkUpdateWithTx(tx, query, args)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// ============== timer with tx ==============
type insertWithTxFunc func(tx *sqlx.Tx, query string, args interface{}) error

func insertWithTxTimer(f insertWithTxFunc) insertWithTxFunc {
	return func(tx *sqlx.Tx, query string, args interface{}) error {
		start := time.Now()
		defer logSlowSQL(start, query, args)
		return f(tx, query, args)
	}
}

// type insertReturnIDWithTxFunc func(tx *sqlx.Tx, query string, args interface{}) (int64, error)
//
// func insertReturnIDWithTxTimer(f insertReturnIDWithTxFunc) insertReturnIDWithTxFunc {
// 	return func(tx *sqlx.Tx, query string, args interface{}) (int64, error) {
// 		start := time.Now()
// 		defer logSlowSQL(start, query, args)
// 		return f(tx, query, args)
// 	}
// }

type bulkInsertWithTxFunc func(tx *sqlx.Tx, query string, args interface{}) error

func bulkInsertWithTxTimer(f bulkInsertWithTxFunc) bulkInsertWithTxFunc {
	return func(tx *sqlx.Tx, query string, args interface{}) error {
		start := time.Now()
		defer logSlowSQL(start, query, args)
		return f(tx, query, args)
	}
}

type bulkInsertReturnIDWithTxFunc func(tx *sqlx.Tx, query string, args interface{}) (int64, error)

func bulkInsertReturnIDWithTxTimer(f bulkInsertReturnIDWithTxFunc) bulkInsertReturnIDWithTxFunc {
	return func(tx *sqlx.Tx, query string, args interface{}) (int64, error) {
		start := time.Now()
		defer logSlowSQL(start, query, args)
		return f(tx, query, args)
	}
}

type execWithTxFunc func(tx *sqlx.Tx, query string, args ...interface{}) error

func execWithTxTimer(f execWithTxFunc) execWithTxFunc {
	return func(tx *sqlx.Tx, query string, args ...interface{}) error {
		start := time.Now()
		defer logSlowSQL(start, query, args)
		// NOTE: must be args...
		return f(tx, query, args...)
	}
}

type deleteReturnRowsWithTxFunc func(tx *sqlx.Tx, query string, args ...interface{}) (int64, error)

func deleteReturnRowsWithTxTimer(f deleteReturnRowsWithTxFunc) deleteReturnRowsWithTxFunc {
	return func(tx *sqlx.Tx, query string, args ...interface{}) (int64, error) {
		start := time.Now()
		defer logSlowSQL(start, query, args)
		// NOTE: must be args...
		return f(tx, query, args...)
	}
}

type updateWithTxFunc func(tx *sqlx.Tx, query string, args interface{}) (int64, error)

func updateWithTxTimer(f updateWithTxFunc) updateWithTxFunc {
	return func(tx *sqlx.Tx, query string, args interface{}) (int64, error) {
		start := time.Now()
		defer logSlowSQL(start, query, args)
		return f(tx, query, args)
	}
}

// ================== raw execute func with tx ==================
//func sqlxExecWithTx(tx *sqlx.Tx, query string, args ...interface{}) error {
//	_, err := tx.Exec(query, args...)
//	return err
//}

func sqlxInsertWithTx(tx *sqlx.Tx, query string, args interface{}) error {
	_, err := tx.NamedExec(query, args)
	return err
}

//func sqlxInsertReturnIDWithTx(tx *sqlx.Tx, query string, args interface{}) (int64, error) {
//	res, err := tx.NamedExec(query, args)
//	if err != nil {
//		return 0, err
//	}
//	return res.LastInsertId()
//}

func sqlxBulkInsertWithTx(tx *sqlx.Tx, query string, args interface{}) error {
	q, arrayArgs, err := bindArray(sqlx.BindType(tx.DriverName()), query, args, tx.Mapper)
	if err != nil {
		return err
	}
	_, err = tx.Exec(q, arrayArgs...)
	return err
}

func sqlxBulkInsertReturnIDWithTx(tx *sqlx.Tx, query string, args interface{}) (int64, error) {
	q, arrayArgs, err := bindArray(sqlx.BindType(tx.DriverName()), query, args, tx.Mapper)
	if err != nil {
		return 0, err
	}
	res, err := tx.Exec(q, arrayArgs...)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func sqlxBulkUpdateWithTx(tx *sqlx.Tx, query string, args interface{}) error {
	// 预编译
	stmt, err := tx.PrepareNamed(query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	argSlice, err := util.ToSlice(args)
	// 转换不成功，说明是非数组，则单个条件
	if err != nil {
		return err
	}

	// 遍历执行
	for _, arg := range argSlice {
		_, err = stmt.Exec(arg)
		if err != nil {
			return err
		}
	}
	return nil
}

func sqlxDeleteWithTx(tx *sqlx.Tx, query string, args ...interface{}) error {
	query, args, err := sqlx.In(query, args...)
	if err != nil {
		return err
	}
	// TODO: 后续考虑是否需要返回删除的数量
	_, err = tx.Exec(query, args...)
	return err
}

func sqlxDeleteReturnRowsWithTx(tx *sqlx.Tx, query string, args ...interface{}) (int64, error) {
	query, args, err := sqlx.In(query, args...)
	if err != nil {
		return 0, err
	}
	result, err := tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rowsAffected, nil
}

func sqlxUpdateWithTx(tx *sqlx.Tx, query string, args interface{}) (int64, error) {
	result, err := tx.NamedExec(query, args)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return rowsAffected, nil
}

// the func after decorate
var (
	SqlxSelect = queryTimer(sqlxSelectFunc)
	SqlxGet    = queryTimer(sqlxGetFunc)

	SqlxDelete     = deleteTimer(sqlxDeleteFunc)
	SqlxUpdate     = updateTimer(sqlxUpdateFunc)
	SqlxBulkInsert = bulkInsertTimer(sqlxBulkInsertFunc)
	SqlxBulkUpdate = bulkInsertTimer(sqlxBulkUpdateFunc)

	SqlxDeleteWithCtx = deleteWithCtxTimer(sqlxDeleteWithCtxFunc)
	SqlxInsertWithTx  = insertWithTxTimer(sqlxInsertWithTx)

	// SqlxInsertReturnIDWithTx     = insertReturnIDWithTxTimer(sqlxInsertReturnIDWithTx)

	SqlxBulkInsertWithTx         = bulkInsertWithTxTimer(sqlxBulkInsertWithTx)
	SqlxBulkInsertReturnIDWithTx = bulkInsertReturnIDWithTxTimer(sqlxBulkInsertReturnIDWithTx)
	SqlxBulkUpdateWithTx         = bulkInsertWithTxTimer(sqlxBulkUpdateWithTx)
	SqlxDeleteWithTx             = execWithTxTimer(sqlxDeleteWithTx)
	SqlxDeleteReturnRowsWithTx   = deleteReturnRowsWithTxTimer(sqlxDeleteReturnRowsWithTx)
	SqlxUpdateWithTx             = updateWithTxTimer(sqlxUpdateWithTx)
	// SqlxExecWithTx               = execWithTxTimer(sqlxExecWithTx)

	// SqlxSensitiveGet will query without timer and logger
	SqlxSensitiveGet = sqlxGetFunc
)
