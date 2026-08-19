/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2023-2024 Artyom Smirnov <artyom_smirnov@me.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*******************************************************************************/

package firebirdsql

import "fmt"

type User struct {
	Username   *string
	Password   *string
	FirstName  *string
	MiddleName *string
	LastName   *string
	UserId     int32
	GroupId    int32
	Admin      *bool
}

type UserManager struct {
	sm         *ServiceManager
	securityDb string
}

type UserManagerOptions struct {
	SecurityDB string
}

type UserManagerOption func(*UserManagerOptions)

func WithSecurityDB(securityDB string) UserManagerOption {
	return func(opts *UserManagerOptions) {
		opts.SecurityDB = securityDB
	}
}

func GetDefaultUserManagerOptions() UserManagerOptions {
	return UserManagerOptions{
		SecurityDB: "",
	}
}

func NewUserManagerOptions(opts ...UserManagerOption) UserManagerOptions {
	res := GetDefaultUserManagerOptions()
	for _, opt := range opts {
		opt(&res)
	}
	return res
}

type UserOption func(*User)

func WithUsername(username string) UserOption {
	return func(opts *User) {
		opts.Username = &username
	}
}

func WithPassword(password string) UserOption {
	return func(opts *User) {
		opts.Password = &password
	}
}

func WithFirstName(firstname string) UserOption {
	return func(opts *User) {
		opts.FirstName = &firstname
	}
}

func WithMiddleName(middlename string) UserOption {
	return func(opts *User) {
		opts.MiddleName = &middlename
	}
}

func WithLastName(lastname string) UserOption {
	return func(opts *User) {
		opts.LastName = &lastname
	}
}

func WithUserId(userId int32) UserOption {
	return func(opts *User) {
		opts.UserId = userId
	}
}

func WithGroupId(groupId int32) UserOption {
	return func(opts *User) {
		opts.GroupId = groupId
	}
}

func WithAdmin() UserOption {
	return func(opts *User) {
		res := true
		opts.Admin = &res
	}
}

func WithoutAdmin() UserOption {
	return func(opts *User) {
		res := false
		opts.Admin = &res
	}
}

func NewUser(opts ...UserOption) User {
	res := User{
		UserId:  -1,
		GroupId: -1,
	}
	for _, opt := range opts {
		opt(&res)
	}
	return res
}

func (u *User) GetSpb() []byte {
	srb := NewXPBWriter()
	if u.Username != nil {
		srb.PutString(isc_spb_sec_username, *u.Username)
	}
	if u.Password != nil {
		srb.PutString(isc_spb_sec_password, *u.Password)
	}
	if u.FirstName != nil {
		srb.PutString(isc_spb_sec_firstname, *u.FirstName)
	}
	if u.MiddleName != nil {
		srb.PutString(isc_spb_sec_middlename, *u.MiddleName)
	}
	if u.LastName != nil {
		srb.PutString(isc_spb_sec_lastname, *u.LastName)
	}
	if u.UserId != -1 {
		srb.PutInt32(isc_spb_sec_userid, u.UserId)
	}
	if u.GroupId != -1 {
		srb.PutInt32(isc_spb_sec_groupid, u.GroupId)
	}
	if u.Admin != nil {
		if *u.Admin {
			srb.PutInt32(isc_spb_sec_admin, 1)
		} else {
			srb.PutInt32(isc_spb_sec_admin, 0)
		}
	}
	return srb.Bytes()
}

func NewUserManager(addr string, user string, password string, smo ServiceManagerOptions, umo UserManagerOptions) (*UserManager, error) {
	var (
		sm  *ServiceManager
		err error
	)

	if sm, err = NewServiceManager(addr, user, password, smo); err != nil {
		return nil, err
	}
	return &UserManager{
		sm,
		umo.SecurityDB,
	}, nil
}

func (um *UserManager) Close() error {
	return um.sm.Close()
}

func (um *UserManager) userAction(action byte, user *User) error {
	spb := NewXPBWriterFromTag(action)
	if user != nil {
		spb.PutBytes(user.GetSpb())
	}
	if um.securityDb != "" {
		spb.PutString(isc_spb_dbname, um.securityDb)
	}
	return um.sm.ServiceAttach(spb.Bytes(), nil)
}

func (um *UserManager) userActionStart(action byte, user *User) error {
	spb := NewXPBWriterFromTag(action)
	if user != nil {
		spb.PutBytes(user.GetSpb())
	}
	if um.securityDb != "" {
		spb.PutString(isc_spb_dbname, um.securityDb)
	}
	return um.sm.ServiceStart(spb.Bytes())
}

func (um *UserManager) AddUser(user User) error {
	err := um.userAction(isc_action_svc_add_user, &user)
	return err
}

func (um *UserManager) DeleteUser(user User) error {
	del := NewUser(WithUsername(*user.Username))
	err := um.userAction(isc_action_svc_delete_user, &del)
	return err
}

func (um *UserManager) ModifyUser(user User) error {
	err := um.userAction(isc_action_svc_modify_user, &user)
	return err
}

func (um *UserManager) GetUsers() ([]User, error) {
	var (
		err     error
		buf     []byte
		resChan = make(chan []byte)
		done    = make(chan bool)
		cont    = true
		users   []User
	)
	if err = um.userActionStart(isc_action_svc_display_user_adm, nil); err != nil {
		return nil, err
	}

	go func() {
		err = um.sm.WaitBuffer(resChan)
		done <- true
	}()

	// Accumulate the whole service stream before parsing: the server splits
	// records at arbitrary chunk boundaries, so a user record can span two chunks
	// and per-chunk parsing would mis-read one that straddles the boundary.
	var acc []byte
	for cont {
		select {
		case buf = <-resChan:
			acc = append(acc, buf...)
		case <-done:
			cont = false
		}
	}
	if err != nil {
		return nil, err
	}

	srb := NewXPBReader(acc)
	var user *User
	for {
		have, val := srb.Next()
		if !have {
			break
		}
		// Every non-username tag belongs to the user opened by the preceding
		// username tag; a stream leading with a field tag would deref a nil user.
		if val != isc_spb_sec_username && user == nil {
			return nil, fmt.Errorf("firebirdsql: user field 0x%02x before any username", val)
		}
		switch val {
		case isc_spb_sec_username:
			if user != nil {
				users = append(users, *user)
			}
			u := NewUser(WithUsername(srb.GetString()))
			user = &u
		case isc_spb_sec_firstname:
			s := srb.GetString()
			user.FirstName = &s
		case isc_spb_sec_middlename:
			s := srb.GetString()
			user.MiddleName = &s
		case isc_spb_sec_lastname:
			s := srb.GetString()
			user.LastName = &s
		case isc_spb_sec_userid:
			user.UserId = srb.GetInt32()
		case isc_spb_sec_groupid:
			user.GroupId = srb.GetInt32()
		case isc_spb_sec_admin:
			a := srb.GetInt32() > 0
			user.Admin = &a
		}
	}
	if err := srb.Err(); err != nil {
		return nil, err
	}
	if user != nil {
		users = append(users, *user)
	}

	return users, nil
}

func (um *UserManager) adminRoleMappingAction(action byte) error {
	spb := NewXPBWriterFromTag(action)
	if um.securityDb != "" {
		spb.PutString(isc_spb_dbname, um.securityDb)
	}
	return um.sm.ServiceAttach(spb.Bytes(), nil)
}

func (um *UserManager) SetAdminRoleMapping() error {
	return um.adminRoleMappingAction(isc_action_svc_set_mapping)
}

func (um *UserManager) DropAdminRoleMapping() error {
	return um.adminRoleMappingAction(isc_action_svc_drop_mapping)
}
